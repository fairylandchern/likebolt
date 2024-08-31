package likebolt

import (
	"bytes"
	"fmt"
	"unsafe"
)

const (
	// MaxKeySize is the maximum length of a key, in bytes.
	MaxKeySize = 32768

	// MaxValueSize is the maximum length of a value, in bytes.
	MaxValueSize = (1 << 31) - 2
)

const (
	maxUint = ^uint(0)
	minUint = 0
	maxInt  = int(^uint(0) >> 1)
	minInt  = -maxInt - 1
)

// 16 byte
const bucketHeaderSize = int(unsafe.Sizeof(bucket{}))

const (
	minFillPercent = 0.1
	maxFillPercent = 1.0
)

// DefaultFillPercent is the percentage that split pages are filled.
// This value can be changed by setting Bucket.FillPercent.
const DefaultFillPercent = 0.5

// 一组key/value的集合，也就是一个b+树
type Bucket struct {
	*bucket
	//tx       *Tx                // the associated transaction
	buckets  map[string]*Bucket // subbucket cache
	page     *page              // inline page reference，内联页引用
	rootNode *node              // materialized node for the root page.它存放的就是b+树的根节点
	nodes    map[pgid]*node     // node cache

	// 填充率
	FillPercent float64

	db *DB
}

// bucket represents the on-file representation of a bucket.
// This is stored as the "value" of a bucket key. If the bucket is small enough,
// then its root page can be stored inline in the "value", after the bucket
// header. In the case of inline buckets, the "root" will be 0.
type bucket struct {
	root     pgid   // page id of the bucket's root-level page
	sequence uint64 // monotonically incrementing, used by NextSequence()
}

// newBucket returns a new bucket associated with a transaction.
//func newBucket(tx *Tx) Bucket {
//	var b = Bucket{tx: tx, FillPercent: DefaultFillPercent}
//	if tx.writable {
//		b.buckets = make(map[string]*Bucket)
//		b.nodes = make(map[pgid]*node)
//	}
//	return b
//}

func newBucket(db *DB) *Bucket {
	return &Bucket{FillPercent: DefaultFillPercent, buckets: make(map[string]*Bucket), nodes: make(map[pgid]*node), db: db}
}

func (b *Bucket) Cursor() *Cursor {
	// Update transaction statistics.
	//b.tx.stats.CursorCount++

	// Allocate and return a cursor.
	return &Cursor{
		bucket: b,
		stack:  make([]elemRef, 0),
	}
}

// pageNode returns the in-memory node, if it exists.
// Otherwise returns the underlying page.
func (b *Bucket) pageNode(id pgid) (*page, *node) {
	// Inline buckets have a fake page embedded in their value so treat them
	// differently. We'll return the rootNode (if available) or the fake page.
	// 内联页的话，就直接返回其page了
	if b.root == 0 {
		if id != 0 {
			panic(fmt.Sprintf("inline bucket non-zero page access(2): %d != 0", id))
		}
		if b.rootNode != nil {
			return nil, b.rootNode
		}
		return b.page, nil
	}

	// Check the node cache for non-inline buckets.
	if b.nodes != nil {
		if n := b.nodes[id]; n != nil {
			return nil, n
		}
	}

	// Finally lookup the page from the transaction if no node is materialized.
	//return b.tx.page(id), nil
	return b.db.page(id), nil
}

// 根据pgid创建一个node
func (b *Bucket) node(pgid pgid, parent *node) *node {
	_assert(b.nodes != nil, "nodes map expected")

	// Retrieve node if it's already been created.
	if n := b.nodes[pgid]; n != nil {
		return n
	}

	// Otherwise create a node and cache it.
	n := &node{bucket: b, parent: parent}
	if parent == nil {
		b.rootNode = n
	} else {
		parent.children = append(parent.children, n)
	}

	// Use the inline page if this is an inline bucket.
	// 如果第二次进来，b.page不为空
	// 此处的pgid和b.page只会有一个是有值的。
	var p = b.page
	// 说明不是内联桶
	if p == nil {
		//p = b.tx.page(pgid)
		p = b.db.pageInfo(pgid)
	}

	// Read the page into the node and cache it.
	n.read(p)
	// 缓存
	b.nodes[pgid] = n

	// Update statistics.
	//b.tx.stats.NodeCount++

	return n
}

// rebalance attempts to balance all nodes.
func (b *Bucket) rebalance() {
	for _, n := range b.nodes {
		n.rebalance()
	}
	for _, child := range b.buckets {
		child.rebalance()
	}
}

// spill writes all the nodes for this bucket to dirty pages.
func (b *Bucket) spill() error {
	// Spill all child buckets first.
	for name, child := range b.buckets {
		// If the child bucket is small enough and it has no child buckets then
		// write it inline into the parent bucket's page. Otherwise spill it
		// like a normal bucket and make the parent value a pointer to the page.
		var value []byte
		if child.inlineable() {
			child.free()
			// 重新更新bucket的val的值
			value = child.write()
		} else {
			if err := child.spill(); err != nil {
				return err
			}

			// Update the child bucket header in this bucket.
			// 记录value
			value = make([]byte, unsafe.Sizeof(bucket{}))
			var bucket = (*bucket)(unsafe.Pointer(&value[0]))
			*bucket = *child.bucket
		}

		// Skip writing the bucket if there are no materialized nodes.
		if child.rootNode == nil {
			continue
		}

		// Update parent node.
		var c = b.Cursor()
		k, _, flags := c.seek([]byte(name))
		if !bytes.Equal([]byte(name), k) {
			panic(fmt.Sprintf("misplaced bucket header: %x -> %x", []byte(name), k))
		}
		if flags&bucketLeafFlag == 0 {
			panic(fmt.Sprintf("unexpected bucket header flag: %x", flags))
		}
		// 更新子桶的value
		c.node().put([]byte(name), []byte(name), value, 0, bucketLeafFlag)
	}

	// Ignore if there's not a materialized root node.
	if b.rootNode == nil {
		return nil
	}

	// Spill nodes.
	if err := b.rootNode.spill(); err != nil {
		return err
	}
	b.rootNode = b.rootNode.root()

	// Update the root node for this bucket.
	//if b.rootNode.pgid >= b.tx.meta.pgid {
	//	panic(fmt.Sprintf("pgid (%d) above high water mark (%d)", b.rootNode.pgid, b.tx.meta.pgid))
	//}
	b.root = b.rootNode.pgid

	return nil
}

// inlineable returns true if a bucket is small enough to be written inline
// and if it contains no subbuckets. Otherwise returns false.
func (b *Bucket) inlineable() bool {
	var n = b.rootNode

	// Bucket must only contain a single leaf node.
	if n == nil || !n.isLeaf {
		return false
	}

	// Bucket is not inlineable if it contains subbuckets or if it goes beyond
	// our threshold for inline bucket size.
	var size = pageHeaderSize
	for _, inode := range n.inodes {
		size += leafPageElementSize + len(inode.key) + len(inode.value)

		if inode.flags&bucketLeafFlag != 0 {
			// 有子桶时，不能内联
			return false
		} else if size > b.maxInlineBucketSize() {
			// 如果长度大于1/4页时，就不内联了
			return false
		}
	}

	return true
}

// Returns the maximum total size of a bucket to make it a candidate for inlining.
func (b *Bucket) maxInlineBucketSize() int {
	//return b.tx.db.pageSize / 4
	return b.db.pageSize / 4
}

// free recursively frees all pages in the bucket.
func (b *Bucket) free() {
	if b.root == 0 {
		return
	}

	//var tx = b.tx
	b.forEachPageNode(func(p *page, n *node, _ int) {
		if p != nil {
			//tx.db.freelist.free(tx.meta.txid, p)
			b.db.freelist.free(p)
		} else {
			n.free()
		}
	})
	b.root = 0
}

// forEachPageNode iterates over every page (or node) in a bucket.
// This also includes inline pages.
func (b *Bucket) forEachPageNode(fn func(*page, *node, int)) {
	// If we have an inline page or root node then just use that.
	if b.page != nil {
		fn(b.page, nil, 0)
		return
	}
	b._forEachPageNode(b.root, 0, fn)
}

func (b *Bucket) _forEachPageNode(pgid pgid, depth int, fn func(*page, *node, int)) {
	var p, n = b.pageNode(pgid)

	// Execute function.
	fn(p, n, depth)

	// Recursively loop over children.
	if p != nil {
		if (p.flags & branchPageFlag) != 0 {
			for i := 0; i < int(p.count); i++ {
				elem := p.branchPageElement(uint16(i))
				b._forEachPageNode(elem.pgid, depth+1, fn)
			}
		}
	} else {
		if !n.isLeaf {
			for _, inode := range n.inodes {
				b._forEachPageNode(inode.pgid, depth+1, fn)
			}
		}
	}
}

// write allocates and writes a bucket to a byte slice.
// 内联桶的话，其value中bucketHeaderSize后面的内容为其page的数据
func (b *Bucket) write() []byte {
	// Allocate the appropriate size.
	var n = b.rootNode
	var value = make([]byte, bucketHeaderSize+n.size())

	// Write a bucket header.
	var bucket = (*bucket)(unsafe.Pointer(&value[0]))
	*bucket = *b.bucket

	// Convert byte slice to a fake page and write the root node.
	var p = (*page)(unsafe.Pointer(&value[bucketHeaderSize]))
	// 将该桶中的元素压缩存储，放在value中
	n.write(p)

	return value
}

// ForEach executes a function for each key/value pair in a bucket.
// If the provided function returns an error then the iteration is stopped and
// the error is returned to the caller. The provided function must not modify
// the bucket; this will result in undefined behavior.
func (b *Bucket) ForEach(fn func(k, v []byte) error) error {
	if b.db == nil {
		return ErrTxClosed
	}
	c := b.Cursor()
	// 遍历键值对
	for k, v := c.First(); k != nil; k, v = c.Next() {
		if err := fn(k, v); err != nil {
			return err
		}
	}
	return nil
}

// Bucket retrieves a nested bucket by name.
// Returns nil if the bucket does not exist.
// The bucket instance is only valid for the lifetime of the transaction.
func (b *Bucket) Bucket(name []byte) *Bucket {
	if b.buckets != nil {
		if child := b.buckets[string(name)]; child != nil {
			return child
		}
	}

	// Move cursor to key.
	// 根据游标找key
	c := b.Cursor()
	k, v, flags := c.seek(name)

	// Return nil if the key doesn't exist or it is not a bucket.
	if !bytes.Equal(name, k) || (flags&bucketLeafFlag) == 0 {
		return nil
	}

	// Otherwise create a bucket and cache it.
	// 根据找到的value来打开桶。
	var child = b.openBucket(v)
	// 加速缓存的作用
	if b.buckets != nil {
		b.buckets[string(name)] = child
	}

	return child
}

// Helper method that re-interprets a sub-bucket value
// from a parent into a Bucket
func (b *Bucket) openBucket(value []byte) *Bucket {
	var child = newBucket(b.db)

	// If unaligned load/stores are broken on this arch and value is
	// unaligned simply clone to an aligned byte array.
	// todo 可能理解就是非对齐内存访问方式不能正常工作时，通过复制数组使其正常工作
	// todo 非对齐内存访问linux kernel组的相关文档的解释
	// todo https://www.kernel.org/doc/html/latest/translations/zh_CN/core-api/unaligned-memory-access.html
	unaligned := brokenUnaligned && uintptr(unsafe.Pointer(&value[0]))&3 != 0

	if unaligned {
		value = cloneBytes(value)
	}

	// If this is a writable transaction then we need to copy the bucket entry.
	// Read-only transactions can point directly at the mmap entry.
	if !unaligned {
		child.bucket = &bucket{}
		*child.bucket = *(*bucket)(unsafe.Pointer(&value[0]))
	} else {
		child.bucket = (*bucket)(unsafe.Pointer(&value[0]))
	}

	// Save a reference to the inline page if the bucket is inline.
	// 内联桶
	if child.root == 0 {
		child.page = (*page)(unsafe.Pointer(&value[bucketHeaderSize]))
	}

	return child
}

// cloneBytes returns a copy of a given slice.
func cloneBytes(v []byte) []byte {
	var clone = make([]byte, len(v))
	copy(clone, v)
	return clone
}
