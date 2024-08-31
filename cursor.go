package likebolt

import (
	"bytes"
	"fmt"
	"sort"
)

type Cursor struct {
	bucket *Bucket
	// 保存遍历搜索的路径
	stack []elemRef
}

// elemRef represents a reference to an element on a given page/node.
type elemRef struct {
	page  *page
	node  *node
	index int
}

// isLeaf returns whether the ref is pointing at a leaf page/node.
func (r *elemRef) isLeaf() bool {
	if r.node != nil {
		return r.node.isLeaf
	}
	return (r.page.flags & leafPageFlag) != 0
}

// count returns the number of inodes or page elements.
func (r *elemRef) count() int {
	if r.node != nil {
		return len(r.node.inodes)
	}
	return int(r.page.count)
}

// seek moves the cursor to a given key and returns it.
// If the key does not exist then the next key is used.
func (c *Cursor) seek(seek []byte) (key []byte, value []byte, flags uint32) {
	//_assert(c.bucket.tx.db != nil, "tx closed")

	// Start from root page/node and traverse to correct page.
	c.stack = c.stack[:0]
	// 开始根据seek的key值搜索root，3
	c.search(seek, c.bucket.root)
	//
	ref := &c.stack[len(c.stack)-1]

	// If the cursor is pointing to the end of page/node then return nil.
	if ref.index >= ref.count() {
		return nil, nil, 0
	}

	// If this is a bucket then return a nil value.
	return c.keyValue()
}

// 从根节点开始遍历
// search recursively performs a binary search against a given page/node until it finds a given key.
func (c *Cursor) search(key []byte, pgid pgid) {
	// root，3
	// 层层找page，bucket->tx->db->dataref
	p, n := c.bucket.pageNode(pgid)
	if p != nil && (p.flags&(branchPageFlag|leafPageFlag)) == 0 {
		panic(fmt.Sprintf("invalid page type: %d: %x", p.id, p.flags))
	}
	e := elemRef{page: p, node: n}
	c.stack = append(c.stack, e)

	// If we're on a leaf page/node then find the specific node.
	// 如果是叶子节点，找具体的node
	if e.isLeaf() {
		c.nsearch(key)
		return
	}

	if n != nil {
		// 先搜索node
		c.searchNode(key, n)
		return
	}
	// 在搜索page
	c.searchPage(key, p)
}

func (c *Cursor) searchNode(key []byte, n *node) {
	var exact bool
	index := sort.Search(len(n.inodes), func(i int) bool {
		// TODO(benbjohnson): Optimize this range search. It's a bit hacky right now.
		// sort.Search() finds the lowest index where f() != -1 but we need the highest index.
		ret := bytes.Compare(n.inodes[i].key, key)
		if ret == 0 {
			exact = true
		}
		return ret != -1
	})
	if !exact && index > 0 {
		index--
	}
	c.stack[len(c.stack)-1].index = index

	// Recursively search to the next page.
	c.search(key, n.inodes[index].pgid)
}

func (c *Cursor) searchPage(key []byte, p *page) {
	// Binary search for the correct range.
	inodes := p.branchPageElements()

	var exact bool
	index := sort.Search(int(p.count), func(i int) bool {
		// TODO(benbjohnson): Optimize this range search. It's a bit hacky right now.
		// sort.Search() finds the lowest index where f() != -1 but we need the highest index.
		ret := bytes.Compare(inodes[i].key(), key)
		if ret == 0 {
			exact = true
		}
		return ret != -1
	})
	if !exact && index > 0 {
		index--
	}
	c.stack[len(c.stack)-1].index = index

	// Recursively search to the next page.
	c.search(key, inodes[index].pgid)
}

// 搜索叶子页
func (c *Cursor) nsearch(key []byte) {
	e := &c.stack[len(c.stack)-1]
	p, n := e.page, e.node

	// If we have a node then search its inodes.
	// 先搜索node
	if n != nil {
		index := sort.Search(len(n.inodes), func(i int) bool {
			return bytes.Compare(n.inodes[i].key, key) != -1
		})
		e.index = index
		return
	}

	// If we have a page then search its leaf elements.
	// 再搜索page
	inodes := p.leafPageElements()
	index := sort.Search(int(p.count), func(i int) bool {
		return bytes.Compare(inodes[i].key(), key) != -1
	})
	e.index = index
}

// keyValue returns the key and value of the current leaf element.
func (c *Cursor) keyValue() ([]byte, []byte, uint32) {
	ref := &c.stack[len(c.stack)-1]
	if ref.count() == 0 || ref.index >= ref.count() {
		return nil, nil, 0
	}

	// Retrieve value from node.
	// 先从内存中取
	if ref.node != nil {
		inode := &ref.node.inodes[ref.index]
		return inode.key, inode.value, inode.flags
	}

	// 其次再从文件page中取
	// Or retrieve value from page.
	elem := ref.page.leafPageElement(uint16(ref.index))
	return elem.key(), elem.value(), elem.flags
}

// node returns the node that the cursor is currently positioned on.
func (c *Cursor) node() *node {
	_assert(len(c.stack) > 0, "accessing a node with a zero-length cursor stack")

	// If the top of the stack is a leaf node then just return it.
	if ref := &c.stack[len(c.stack)-1]; ref.node != nil && ref.isLeaf() {
		return ref.node
	}

	// Start from root and traverse down the hierarchy.
	var n = c.stack[0].node
	if n == nil {
		n = c.bucket.node(c.stack[0].page.id, nil)
	}
	// 非叶子节点
	for _, ref := range c.stack[:len(c.stack)-1] {
		_assert(!n.isLeaf, "expected branch node")
		n = n.childAt(int(ref.index))
	}
	_assert(n.isLeaf, "expected leaf node")
	return n
}

// First moves the cursor to the first item in the bucket and returns its key and value.
// If the bucket is empty then a nil key and value are returned.
// The returned key and value are only valid for the life of the transaction.
func (c *Cursor) First() (key []byte, value []byte) {
	_assert(c.bucket.db != nil, "tx closed")
	// 清空stack
	c.stack = c.stack[:0]
	p, n := c.bucket.pageNode(c.bucket.root)

	// 一直找到第一个叶子节点，此处在添加stack时，一直让index设置为0即可
	ref := elemRef{page: p, node: n, index: 0}
	c.stack = append(c.stack, ref)

	c.first()

	// If we land on an empty page then move to the next value.
	// https://github.com/boltdb/bolt/issues/450
	// 当前页时空的话，找下一个
	if c.stack[len(c.stack)-1].count() == 0 {
		c.next()
	}

	k, v, flags := c.keyValue()
	// 是桶
	if (flags & uint32(bucketLeafFlag)) != 0 {
		return k, nil
	}
	return k, v

}

// first moves the cursor to the first leaf element under the last page in the stack.
// 找到最后一个非叶子节点的第一个叶子节点。index=0的节点
func (c *Cursor) first() {
	for {
		// Exit when we hit a leaf page.
		var ref = &c.stack[len(c.stack)-1]
		if ref.isLeaf() {
			break
		}

		// Keep adding pages pointing to the first element to the stack.
		var pgid pgid
		if ref.node != nil {
			pgid = ref.node.inodes[ref.index].pgid
		} else {
			pgid = ref.page.branchPageElement(uint16(ref.index)).pgid
		}
		p, n := c.bucket.pageNode(pgid)
		c.stack = append(c.stack, elemRef{page: p, node: n, index: 0})
	}
}

// next moves to the next leaf element and returns the key and value.
// If the cursor is at the last leaf element then it stays there and returns nil.
func (c *Cursor) next() (key []byte, value []byte, flags uint32) {
	for {
		// Attempt to move over one element until we're successful.
		// Move up the stack as we hit the end of each page in our stack.
		var i int
		for i = len(c.stack) - 1; i >= 0; i-- {
			elem := &c.stack[i]
			if elem.index < elem.count()-1 {
				// 元素还有时，往后移动一个
				elem.index++
				break
			}
		}
		// 最后的结果elem.index++

		// If we've hit the root page then stop and return. This will leave the
		// cursor on the last element of the last page.
		// 所有页都遍历完了
		if i == -1 {
			return nil, nil, 0
		}

		// Otherwise start from where we left off in the stack and find the
		// first element of the first leaf page.
		// 剩余的节点里面找，跳过原先遍历过的节点
		c.stack = c.stack[:i+1]
		// 如果是叶子节点，first()啥都不做，直接退出。返回elem.index+1的数据
		// 非叶子节点的话，需要移动到stack中最后一个路径的第一个元素
		c.first()

		// If this is an empty page then restart and move back up the stack.
		// https://github.com/boltdb/bolt/issues/450
		if c.stack[len(c.stack)-1].count() == 0 {
			continue
		}

		return c.keyValue()
	}
}

// Next moves the cursor to the next item in the bucket and returns its key and value.
// If the cursor is at the end of the bucket then a nil key and value are returned.
// The returned key and value are only valid for the life of the transaction.
func (c *Cursor) Next() (key []byte, value []byte) {
	_assert(c.bucket.db != nil, "tx closed")
	k, v, flags := c.next()
	if (flags & uint32(bucketLeafFlag)) != 0 {
		return k, nil
	}
	return k, v
}
