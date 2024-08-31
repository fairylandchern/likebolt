package likebolt

import (
	"fmt"
	"sort"
	"unsafe"
)

// freelist represents a list of all pages that are available for allocation.
// It also tracks pages that have been freed but are still in use by open transactions.
type freelist struct {
	// 已经可以被分配的空闲页
	ids []pgid // all free and available free page ids.
	// 将来很快能被释放的空闲页，部分事务可能在读或者写
	//pending map[txid][]pgid // mapping of soon-to-be free page ids by tx.
	pending []pgid
	cache   map[pgid]bool // fast lookup of all free and pending page ids.
}

// newFreelist returns an empty, initialized freelist.
func newFreelist() *freelist {
	return &freelist{
		//pending: make(map[txid][]pgid),
		pending: make([]pgid, 0),
		cache:   make(map[pgid]bool),
	}
}

// 从磁盘中加载空闲页信息，并转为freelist结构，转换时，也需要注意其空闲页的个数的判断逻辑，
// 当p.count为0xFFFF时，需要读取p.ptr中的第一个字节来计算其空闲页的个数。否则则直接读取p.ptr中存放的数据为空闲页ids列表
func (f *freelist) read(p *page) {
	// If the page.count is at the max uint16 value (64k) then it's considered
	// an overflow and the size of the freelist is stored as the first element.
	idx, count := 0, int(p.count)
	if count == 0xFFFF {
		idx = 1
		// 用第一个uint64来存储整个count的值
		count = int(((*[maxAllocSize]pgid)(unsafe.Pointer(&p.ptr)))[0])
	}

	// Copy the list of page ids from the freelist.
	if count == 0 {
		f.ids = nil
	} else {
		ids := ((*[maxAllocSize]pgid)(unsafe.Pointer(&p.ptr)))[idx:count]
		f.ids = make([]pgid, len(ids))
		copy(f.ids, ids)

		// Make sure they're sorted.
		sort.Sort(pgids(f.ids))
	}

	// Rebuild the page cache.
	f.reindex()
}

// reindex rebuilds the free cache based on available and pending free lists.
func (f *freelist) reindex() {
	f.cache = make(map[pgid]bool, len(f.ids))
	for _, id := range f.ids {
		f.cache[id] = true
	}
	// todo 暂时移除tx相关逻辑
	//for _, pendingIDs := range f.pending {
	//for _, pendingID := range pendingIDs {
	//	f.cache[pendingID] = true
	//}
	//}

	for _, pendingID := range f.pending {
		f.cache[pendingID] = true
	}
}

// 将页page和tx关联起来
// free releases a page and its overflow for a given transaction id.
// If the page is already free then a panic will occur.
//func (f *freelist) free(txid txid, p *page) {
//	if p.id <= 1 {
//		panic(fmt.Sprintf("cannot free page 0 or 1: %d", p.id))
//	}
//
//	// Free page and all its overflow pages.
//	var ids = f.pending[txid]
//	for id := p.id; id <= p.id+pgid(p.overflow); id++ {
//		// Verify that page is not already free.
//		if f.cache[id] {
//			panic(fmt.Sprintf("page %d already freed", id))
//		}
//
//		// Add to the freelist and cache.
//		ids = append(ids, id)
//		f.cache[id] = true
//	}
//	f.pending[txid] = ids
//}

func (f *freelist) free(p *page) {
	if p.id <= 1 {
		panic(fmt.Sprintf("cannot free page 0 or 1: %d", p.id))
	}
	ids := f.pending
	for id := p.id; id <= p.id+pgid(p.overflow); id++ {
		// Verify that page is not already free.
		if f.cache[id] {
			panic(fmt.Sprintf("page %d already freed", id))
		}
		// Add to the freelist and cache.
		ids = append(ids, id)
		f.cache[id] = true
	}
	f.pending = ids
}

// allocate returns the starting page id of a contiguous list of pages of a given size.
// If a contiguous block cannot be found then 0 is returned.
// [5,6,7,13,14,15,16,18,19,20,31,32]
// 开始分配一段连续的n个页。其中返回值为初始的页id。如果无法分配，则返回0即可
func (f *freelist) allocate(n int) pgid {
	if len(f.ids) == 0 {
		return 0
	}

	var initial, previd pgid
	for i, id := range f.ids {
		if id <= 1 {
			panic(fmt.Sprintf("invalid page allocation: %d", id))
		}

		// Reset initial page if this is not contiguous.
		// id-previd != 1 来判断是否连续
		if previd == 0 || id-previd != 1 {
			// 第一次不连续时记录一下第一个位置
			initial = id
		}

		// If we found a contiguous block then remove it and return it.
		// 找到了连续的块，然后将其返回即可
		if (id-initial)+1 == pgid(n) {
			// If we're allocating off the beginning then take the fast path
			// and just adjust the existing slice. This will use extra memory
			// temporarily but the append() in free() will realloc the slice
			// as is necessary.
			if (i + 1) == n {
				// 找到的是前n个连续的空间
				f.ids = f.ids[i+1:]
			} else {
				copy(f.ids[i-n+1:], f.ids[i+1:])
				f.ids = f.ids[:len(f.ids)-n]
			}

			// Remove from the free cache.
			// 同时更新缓存
			for i := pgid(0); i < pgid(n); i++ {
				delete(f.cache, initial+i)
			}

			return initial
		}

		previd = id
	}
	return 0
}

// size returns the size of the page after serialization.
func (f *freelist) size() int {
	n := f.count()
	// 溢出
	// 2^16=64k
	if n >= 0xFFFF {
		// The first element will be used to store the count. See freelist.write.
		n++
	}
	return pageHeaderSize + (int(unsafe.Sizeof(pgid(0))) * n)
}

// count returns count of pages on the freelist
func (f *freelist) count() int {
	return f.free_count() + f.pending_count()
}

// free_count returns count of free pages
func (f *freelist) free_count() int {
	return len(f.ids)
}

// pending_count returns count of pending pages
func (f *freelist) pending_count() int {
	var count int
	//for _, list := range f.pending {
	//	count += len(list)
	//}
	count = len(f.pending)
	return count
}

// write writes the page ids onto a freelist page. All free and pending ids are
// saved to disk since in the event of a program crash, all pending ids will
// become free.
// 将空闲列表转换成页信息，写到磁盘中，此处需要注意一个问题，页头中的count字段是一个uint16，占两个字节，
// 其最大可以表示2^16即65536个数字，当空闲页的个数超过65535时时，需要将p.ptr中的第一个字节用来存储其
// 空闲页的个数，同时将p.count设置为0xFFFF。否则不超过的情况下，直接用count来表示其空闲页的个数
func (f *freelist) write(p *page) error {
	// Combine the old free pgids and pgids waiting on an open transaction.

	// Update the header flag.
	p.flags |= freelistPageFlag

	// The page.count can only hold up to 64k elements so if we overflow that
	// number then we handle it by putting the size in the first element.
	lenids := f.count()
	if lenids == 0 {
		p.count = uint16(lenids)
	} else if lenids < 0xFFFF {
		p.count = uint16(lenids)
		// 拷贝到page的ptr中
		f.copyall(((*[maxAllocSize]pgid)(unsafe.Pointer(&p.ptr)))[:])
	} else {
		// 有溢出的情况下，后面第一个元素放置其长度，然后再存放所有的pgid列表
		p.count = 0xFFFF
		((*[maxAllocSize]pgid)(unsafe.Pointer(&p.ptr)))[0] = pgid(lenids)
		// 从第一个元素位置拷贝
		//pgids:=((*[maxAllocSize]pgid)(unsafe.Pointer(&p.ptr)))[1:]
		f.copyall(((*[maxAllocSize]pgid)(unsafe.Pointer(&p.ptr)))[1:])
	}
	return nil
}

// copyall copies into dst a list of all free ids and all pending ids in one sorted list.
// f.count returns the minimum length required for dst.
func (f *freelist) copyall(dst []pgid) {
	m := make(pgids, 0, f.pending_count())
	//for _, list := range f.pending {
	//	m = append(m, list...)
	//}
	for _, v := range f.pending {
		m = append(m, v)
	}
	sort.Sort(m)
	// 合并两个有序的列表，最后结果输出到dst中
	mergepgids(dst, f.ids, m)
}
