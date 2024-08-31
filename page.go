package likebolt

import (
	"fmt"
	"hash/fnv"
	"sort"
	"unsafe"
)

// 16字节
const pageHeaderSize = int(unsafe.Offsetof(((*page)(nil)).ptr))

const branchPageElementSize = int(unsafe.Sizeof(branchPageElement{}))
const leafPageElementSize = int(unsafe.Sizeof(leafPageElement{}))

const minKeysPerPage = 2

const (
	branchPageFlag   = 0x01
	leafPageFlag     = 0x02
	metaPageFlag     = 0x04
	freelistPageFlag = 0x10
)

const (
	bucketLeafFlag = 0x01
)

type pgid uint64

type pgids []pgid

func (s pgids) Len() int           { return len(s) }
func (s pgids) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }
func (s pgids) Less(i, j int) bool { return s[i] < s[j] }

type pages []*page

func (s pages) Len() int           { return len(s) }
func (s pages) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }
func (s pages) Less(i, j int) bool { return s[i].id < s[j].id }

type page struct {
	// 页id 8字节
	id pgid
	// flags：页类型，可以是分支，叶子节点，元信息，空闲列表  2字节
	flags uint16
	// 个数 2字节
	count uint16
	// 4字节
	overflow uint32
	//
	ptr uintptr
}

// txid represents the internal transaction identifier.
// todo 是否可移除
type txid uint64

type meta struct {
	magic    uint32
	version  uint32
	pageSize uint32
	flags    uint32
	root     bucket
	freelist pgid
	pgid     pgid
	txid     txid
	checksum uint64
}

// meta returns a pointer to the metadata section of the page.
func (p *page) meta() *meta {
	// 将p.ptr转为meta信息
	return (*meta)(unsafe.Pointer(&p.ptr))
}

// generates the checksum for the meta.
func (m *meta) sum64() uint64 {
	var h = fnv.New64a()
	_, _ = h.Write((*[unsafe.Offsetof(meta{}.checksum)]byte)(unsafe.Pointer(m))[:])
	return h.Sum64()
}

func (m *meta) validate() error {
	if m.magic != magic {
		return ErrInvalid
	} else if m.version != version {
		return ErrVersionMismatch
	} else if m.checksum != 0 && m.checksum != m.sum64() {
		return ErrChecksum
	}
	return nil
}

// branchPageElement represents a node on a branch page.
type branchPageElement struct {
	pos   uint32
	ksize uint32
	pgid  pgid
}

// branchPageElement retrieves the branch node by index
func (p *page) branchPageElement(index uint16) *branchPageElement {
	return &((*[0x7FFFFFF]branchPageElement)(unsafe.Pointer(&p.ptr)))[index]
}

// branchPageElements retrieves a list of branch nodes.
func (p *page) branchPageElements() []branchPageElement {
	if p.count == 0 {
		return nil
	}
	return ((*[0x7FFFFFF]branchPageElement)(unsafe.Pointer(&p.ptr)))[:]
}

// key returns a byte slice of the node key.
func (n *branchPageElement) key() []byte {
	buf := (*[maxAllocSize]byte)(unsafe.Pointer(n))
	// pos~ksize
	return (*[maxAllocSize]byte)(unsafe.Pointer(&buf[n.pos]))[:n.ksize]
}

// leafPageElement represents a node on a leaf page.
// 叶子节点既存储key，也存储value
type leafPageElement struct {
	flags uint32
	pos   uint32
	ksize uint32
	vsize uint32
}

// 叶子节点的key
// key returns a byte slice of the node key.
func (n *leafPageElement) key() []byte {
	buf := (*[maxAllocSize]byte)(unsafe.Pointer(n))
	// pos~ksize
	return (*[maxAllocSize]byte)(unsafe.Pointer(&buf[n.pos]))[:n.ksize:n.ksize]
}

// 叶子节点的value
// value returns a byte slice of the node value.
func (n *leafPageElement) value() []byte {
	buf := (*[maxAllocSize]byte)(unsafe.Pointer(n))
	// key:pos~ksize
	// value:pos+ksize~pos+ksize+vsize
	return (*[maxAllocSize]byte)(unsafe.Pointer(&buf[n.pos+n.ksize]))[:n.vsize:n.vsize]
}

// leafPageElement retrieves the leaf node by index
func (p *page) leafPageElement(index uint16) *leafPageElement {
	n := &((*[0x7FFFFFF]leafPageElement)(unsafe.Pointer(&p.ptr)))[index]

	// 最原始的指针：unsafe.Pointer(&p.ptr)
	// 将其转为(*[0x7FFFFFF]leafPageElement)(unsafe.Pointer(&p.ptr))
	// 然后再取第index个元素 ((*[0x7FFFFFF]leafPageElement)(unsafe.Pointer(&p.ptr)))[index]
	// 最后返回该元素指针 &((*[0x7FFFFFF]leafPageElement)(unsafe.Pointer(&p.ptr)))[index]

	// ((*[0x7FFFFFF]leafPageElement)(unsafe.Pointer(&p.ptr)))
	// (*[0x7FFFFFF]leafPageElement)(unsafe.Pointer(&p.ptr))==>[]leafPageElement
	// &leafElements[index]
	return n
}

// leafPageElements retrieves a list of leaf nodes.
func (p *page) leafPageElements() []leafPageElement {
	if p.count == 0 {
		return nil
	}
	return ((*[0x7FFFFFF]leafPageElement)(unsafe.Pointer(&p.ptr)))[:]
}

// mergepgids copies the sorted union of a and b into dst.
// If dst is too small, it panics.
// 将a和b按照有序合并成到dst中，a和b有序
func mergepgids(dst, a, b pgids) {
	if len(dst) < len(a)+len(b) {
		panic(fmt.Errorf("mergepgids bad len %d < %d + %d", len(dst), len(a), len(b)))
	}
	// Copy in the opposite slice if one is nil.
	if len(a) == 0 {
		copy(dst, b)
		return
	}
	if len(b) == 0 {
		copy(dst, a)
		return
	}

	// Merged will hold all elements from both lists.
	merged := dst[:0]

	// Assign lead to the slice with a lower starting value, follow to the higher value.
	lead, follow := a, b
	if b[0] < a[0] {
		lead, follow = b, a
	}

	// Continue while there are elements in the lead.
	for len(lead) > 0 {
		// Merge largest prefix of lead that is ahead of follow[0].
		n := sort.Search(len(lead), func(i int) bool { return lead[i] > follow[0] })
		merged = append(merged, lead[:n]...)
		if n >= len(lead) {
			break
		}

		// Swap lead and follow.
		lead, follow = follow, lead[n:]
	}

	// Append what's left in follow.
	_ = append(merged, follow...)
}

// typ returns a human readable page type string used for debugging.
func (p *page) typ() string {
	if (p.flags & branchPageFlag) != 0 {
		return "branch"
	} else if (p.flags & leafPageFlag) != 0 {
		return "leaf"
	} else if (p.flags & metaPageFlag) != 0 {
		return "meta"
	} else if (p.flags & freelistPageFlag) != 0 {
		return "freelist"
	}
	return fmt.Sprintf("unknown<%02x>", p.flags)
}

// write writes the meta onto a page.
func (m *meta) write(p *page) {
	if m.root.root >= m.pgid {
		panic(fmt.Sprintf("root bucket pgid (%d) above high water mark (%d)", m.root.root, m.pgid))
	} else if m.freelist >= m.pgid {
		panic(fmt.Sprintf("freelist pgid (%d) above high water mark (%d)", m.freelist, m.pgid))
	}

	// Page id is either going to be 0 or 1 which we can determine by the transaction ID.
	p.id = pgid(m.txid % 2)
	p.flags |= metaPageFlag

	// Calculate the checksum.
	m.checksum = m.sum64()

	m.copy(p.meta())
}

// copy copies one meta object to another.
func (m *meta) copy(dest *meta) {
	*dest = *m
}
