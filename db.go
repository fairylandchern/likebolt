package likebolt

import (
	"bytes"
	"fmt"
	"os"
	"runtime"
	"sort"
	"strings"
	"sync"
	"time"
	"unsafe"
)

const (
	maxMapSize   = 0xFFFFFFFFFFFF
	maxAllocSize = 0x7FFFFFFF

	version        = 2
	magic   uint32 = 0xED0CDAED
)

const IgnoreNoSync = runtime.GOOS == "openbsd"

type DB struct {
	file *os.File
	path string
	// mmap读取数据
	data    *[maxMapSize]byte
	dataref []byte
	datasz  int

	filesz int // current on disk file size

	// AllocSize is the amount of space allocated when the database
	// needs to create new pages. This is done to amortize the cost
	// of truncate() and fsync() when growing the data file.
	AllocSize int

	// Setting the NoSync flag will cause the database to skip fsync()
	// calls after each commit. This can be useful when bulk loading data
	// into a database and you can restart the bulk load in the event of
	// a system failure or database corruption. Do not set this flag for
	// normal use.
	//
	// If the package global IgnoreNoSync constant is true, this value is
	// ignored.  See the comment on that constant for more details.
	//
	// THIS IS UNSAFE. PLEASE USE WITH CAUTION.
	NoSync bool

	// When enabled, the database will perform a Check() after every commit.
	// A panic is issued if the database is in an inconsistent state. This
	// flag has a large performance impact so it should only be used for
	// debugging purposes.
	StrictMode bool

	// 初始化，按页读取数据
	pageSize int
	meta0    *meta
	meta1    *meta

	usedMeta *meta

	pagePool sync.Pool

	freelist *freelist // 空闲列表

	// todo 需要对其进行初始化,原本在tx中的逻辑
	root  *Bucket
	pages map[pgid]*page

	ops struct {
		writeAt func(b []byte, off int64) (n int, err error)
	}

	// Read only mode.
	// When true, Update() and Begin(true) return ErrDatabaseReadOnly immediately.
	readOnly bool

	// When true, skips the truncate call when growing the database.
	// Setting this to true is only safe on non-ext3/ext4 systems.
	// Skipping truncation avoids preallocation of hard drive space and
	// bypasses a truncate() and fsync() syscall on remapping.
	//
	// https://github.com/boltdb/bolt/issues/284
	NoGrowSync bool
}

func InitDB(path string) *DB {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		panic(err)
	}
	db := &DB{file: f}
	db.ops.writeAt = db.file.WriteAt
	db.path = path

	info, err := db.file.Stat()
	if err != nil {
		fmt.Println("err path stat:", err)
		return nil
	}
	sz := info.Size()
	if sz == 0 {
		err = db.init()
		if err != nil {
			fmt.Println("err size zero,db init:", err)
			return nil
		}
	} else {
		// 不是新文件，读取第一页元数据
		// Read the first meta page to determine the page size.
		// 2^12,正好是4k
		var buf [0x1000]byte
		if _, err := db.file.ReadAt(buf[:], 0); err == nil {
			// 仅仅是读取了pageSize
			m := db.pageInBuffer(buf[:], 0).meta()
			if err := m.validate(); err != nil {
				// If we can't read the page size, we can assume it's the same
				// as the OS -- since that's how the page size was chosen in the
				// first place.
				//
				// If the first page is invalid and this OS uses a different
				// page size than what the database was created with then we
				// are out of luck and cannot access the database.
				db.pageSize = os.Getpagesize()
			} else {
				db.pageSize = int(m.pageSize)
			}
		}
	}

	// Initialize page pool.
	db.pagePool = sync.Pool{
		New: func() interface{} {
			// 4k
			return make([]byte, db.pageSize)
		},
	}
	// mmap映射db文件数据到内存
	if err := db.mmap(); err != nil {
		fmt.Println("err init mmap:", err)
		return nil
	}

	//todo 暂时用来替换tx中meta
	db.usedMeta = db.meta()

	// Read in the freelist.
	db.freelist = newFreelist()
	// db.meta().freelist=2
	// 读第二页的数据
	// 然后建立起freelist中
	db.freelist.read(db.page(db.usedMeta.freelist))

	// Mark the database as opened and return.
	//todo 添加bucket初始化相关的逻辑
	db.root = newBucket(db)
	db.root.bucket = &db.usedMeta.root
	db.pages = make(map[pgid]*page)

	return db
}

func (db *DB) init() error {
	db.pageSize = os.Getpagesize()
	// Create two meta pages on a buffer.
	buf := make([]byte, db.pageSize*4)
	for i := 0; i < 2; i++ {
		p := db.pageInBuffer(buf[:], pgid(i))
		p.id = pgid(i)
		// 第0页和第1页存放元数据
		p.flags = metaPageFlag

		// Initialize the meta page.
		m := p.meta()
		m.magic = magic
		m.version = version
		m.pageSize = uint32(db.pageSize)
		m.freelist = 2
		m.root = bucket{root: 3}
		m.pgid = 4
		m.txid = txid(i)
		m.checksum = m.sum64()
	}

	// 拿到第2页存放freelist
	p := db.pageInBuffer(buf[:], pgid(2))
	p.id = pgid(2)
	p.flags = freelistPageFlag
	p.count = 0

	// 第三块存放叶子page
	// Write an empty leaf page at page 4.
	p = db.pageInBuffer(buf[:], pgid(3))
	p.id = pgid(3)
	p.flags = leafPageFlag
	p.count = 0

	// 写入4页的数据
	if _, err := db.ops.writeAt(buf, 0); err != nil {
		return err
	}
	// 刷盘
	if err := fdatasync(db); err != nil {
		return err
	}
	return nil
}

func (db *DB) pageInBuffer(b []byte, id pgid) *page {
	return (*page)(unsafe.Pointer(&b[id*pgid(db.pageSize)]))
}

// page retrieves a page reference from the mmap based on the current page size.
func (db *DB) page(id pgid) *page {
	pos := id * pgid(db.pageSize)
	return (*page)(unsafe.Pointer(&db.data[pos]))
}

// 缓存中的page信息
func (db *DB) pageInfo(id pgid) *page {
	// Check the dirty pages first.
	if db.pages != nil {
		if p, ok := db.pages[id]; ok {
			return p
		}
	}

	// Otherwise return directly from the mmap.
	return db.page(id)
}

func (db *DB) mmap() error {
	info, err := db.file.Stat()
	if err != nil {
		return fmt.Errorf("mmap stat error: %s", err)
	} else if int(info.Size()) < db.pageSize*2 {
		return fmt.Errorf("file size too small")
	}
	var size = int(info.Size())
	// Unmap existing data before continuing.
	if err := db.munmap(); err != nil {
		return err
	}

	// Memory-map the data file as a byte slice.
	if err := mmap(db, size); err != nil {
		return err
	}

	// 获取元数据信息
	db.meta0 = db.page(0).meta()
	db.meta1 = db.page(1).meta()

	// Validate the meta pages. We only return an error if both meta pages fail
	// validation, since meta0 failing validation means that it wasn't saved
	// properly -- but we can recover using meta1. And vice-versa.
	err0 := db.meta0.validate()
	err1 := db.meta1.validate()
	if err0 != nil && err1 != nil {
		return err0
	}

	return nil
}

func (db *DB) munmap() error {
	if err := munmap(db); err != nil {
		return fmt.Errorf("unmap error: " + err.Error())
	}
	return nil
}

// meta retrieves the current meta page reference.
func (db *DB) meta() *meta {
	// We have to return the meta with the highest txid which doesn't fail
	// validation. Otherwise, we can cause errors when in fact the database is
	// in a consistent state. metaA is the one with the higher txid.
	metaA := db.meta0
	metaB := db.meta1
	//if db.meta1.txid > db.meta0.txid {
	//	metaA = db.meta1
	//	metaB = db.meta0
	//}

	// Use higher meta page if valid. Otherwise fallback to previous, if valid.
	if err := metaA.validate(); err == nil {
		return metaA
	} else if err := metaB.validate(); err == nil {
		return metaB
	}

	// This should never be reached, because both meta1 and meta0 were validated
	// on mmap() and we do fsync() on every write.
	panic("bolt.DB.meta(): invalid meta pages")
}

// todo 先定义好几个操作数据的接口，后续再考虑怎么结合bolt的源码实现逻辑
// 查询数据
func (db *DB) Get(key []byte) []byte {
	k, v, flags := db.root.Cursor().seek(key)

	// Return nil if this is a bucket.
	if (flags & bucketLeafFlag) != 0 {
		return nil
	}

	// If our target node isn't the same key as what's passed in then return nil.
	if !bytes.Equal(key, k) {
		return nil
	}
	return v
}

// 更新或者插入数据
func (db *DB) Put(key []byte, value []byte) error {
	//if b.tx.db == nil {
	//	return ErrTxClosed
	//} else if !b.Writable() {
	//	return ErrTxNotWritable
	//} else
	if len(key) == 0 {
		return ErrKeyRequired
	} else if len(key) > MaxKeySize {
		return ErrKeyTooLarge
	} else if int64(len(value)) > MaxValueSize {
		return ErrValueTooLarge
	}

	// Move cursor to correct position.
	c := db.root.Cursor()
	k, _, flags := c.seek(key)

	// Return an error if there is an existing key with a bucket value.
	if bytes.Equal(key, k) && (flags&bucketLeafFlag) != 0 {
		return ErrIncompatibleValue
	}

	// Insert into node.
	key = cloneBytes(key)
	c.node().put(key, key, value, 0, 0)

	// todo 添加对子树进行处理的逻辑
	return nil
}

func (db *DB) Delete(key []byte) error {
	//if b.tx.db == nil {
	//	return ErrTxClosed
	//} else if !b.Writable() {
	//	return ErrTxNotWritable
	//}

	// Move cursor to correct position.
	c := db.root.Cursor()
	_, _, flags := c.seek(key)

	// Return an error if there is already existing bucket value.
	if (flags & bucketLeafFlag) != 0 {
		return ErrIncompatibleValue
	}

	// Delete the node if we have a matching key.
	c.node().del(key)

	//todo 添加对子树调整的逻辑，即把tx中的一些逻辑移植到这里，让b+树可以自动对大小进行调节的操作,或者参考commit的逻辑，将后续处理的逻辑外移
	return nil
}

// todo 参考tx逻辑，插入数据后续处理逻辑
func (db *DB) Commit() error {
	// 删除时，进行平衡，页合并
	// Rebalance nodes which have had deletions.
	var startTime = time.Now()
	db.root.rebalance()
	reblanceTm := time.Since(startTime)
	fmt.Println("reblance tm:", reblanceTm)

	// 页分裂，先将node进行分裂，否则的话数据就会有溢出的风险
	// spill data onto dirty pages.
	startTime = time.Now()
	// 这个内部会往缓存tx.pages中加page
	if err := db.root.spill(); err != nil {
		//tx.rollback()
		return err
	}
	spillTm := time.Since(startTime)
	fmt.Println("spill time:", spillTm)

	db.usedMeta.root.root = db.root.root
	opgid := db.usedMeta.pgid

	// 分配新的页面给freelist，然后将freelist写入新的页面
	db.freelist.free(db.pageInfo(db.usedMeta.freelist))
	// 空闲列表可能会增加，因此需要重新分配页用来存储空闲列表
	p, err := db.Allocate((db.freelist.size() / db.pageSize) + 1)
	if err != nil {
		//tx.rollback()
		return err
	}
	// 将freelist写入到连续的新页中
	if err := db.freelist.write(p); err != nil {
		//tx.rollback()
		return err
	}
	db.usedMeta.freelist = p.id
	// 在allocate中有可能会更改meta.pgid
	if db.usedMeta.pgid > opgid {
		if err := db.grow(int(db.usedMeta.pgid+1) * db.pageSize); err != nil {
			//tx.rollback()
			return err
		}
	}

	// Write dirty pages to disk.
	startTime = time.Now()
	// 写数据
	if err := db.write(); err != nil {
		//tx.rollback()
		return err
	}

	// If strict mode is enabled then perform a consistency check.
	// Only the first consistency error is reported in the panic.
	if db.StrictMode {
		ch := db.Check()
		var errs []string
		for {
			err, ok := <-ch
			if !ok {
				break
			}
			errs = append(errs, err.Error())
		}
		if len(errs) > 0 {
			panic("check fail: " + strings.Join(errs, "\n"))
		}
	}

	// Write meta to disk.
	// 元信息写入到磁盘
	if err := db.writeMeta(); err != nil {
		//tx.rollback()
		return err
	}
	//tx.stats.WriteTime += time.Since(startTime)
	writeTime := time.Since(startTime)
	fmt.Println("write time:", writeTime)

	// Finalize the transaction.
	//tx.close()
	return nil
}

// 分配一段连续的页
func (db *DB) Allocate(count int) (*page, error) {
	//p, err := tx.db.allocate(count)
	p, err := db.allocate(count)
	if err != nil {
		return nil, err
	}

	// Save to our page cache.
	db.pages[p.id] = p

	//// Update statistics.
	//tx.stats.PageCount++
	//tx.stats.PageAlloc += count * tx.db.pageSize

	return p, nil
}

// allocate returns a contiguous block of memory starting at a given page.
func (db *DB) allocate(count int) (*page, error) {
	// Allocate a temporary buffer for the page.
	var buf []byte
	if count == 1 {
		buf = db.pagePool.Get().([]byte)
	} else {
		buf = make([]byte, count*db.pageSize)
	}
	// 转成*page
	p := (*page)(unsafe.Pointer(&buf[0]))
	p.overflow = uint32(count - 1)

	// Use pages from the freelist if they are available.
	// 先从空闲列表中找
	if p.id = db.freelist.allocate(count); p.id != 0 {
		return p, nil
	}

	// 找不到的话，就按照事务的pgid来分配
	// 表示需要从文件内部扩大

	// Resize mmap() if we're at the end.
	//p.id = db.rwtx.meta.pgid
	p.id = db.usedMeta.pgid
	// 因此需要判断是否目前所有的页数已经大于了mmap映射出来的空间
	// 这儿计算的页面总数是从当前的id后还要计算count+1个
	var minsz = int((p.id+pgid(count))+1) * db.pageSize
	if minsz >= db.datasz {
		if err := db.mmap(); err != nil {
			return nil, fmt.Errorf("mmap allocate error: %s", err)
		}
	}

	// Move the page id high water mark.
	// 如果不是从freelist中找到的空间的话，更新meta的id，也就意味着是从文件中新扩展的页
	//db.rwtx.meta.pgid += pgid(count)
	db.usedMeta.pgid += pgid(count)
	return p, nil
}

// grow grows the size of the database to the given sz.
func (db *DB) grow(sz int) error {
	// Ignore if the new size is less than available file size.
	if sz <= db.filesz {
		return nil
	}

	// 满足这个条件sz>filesz

	// If the data is smaller than the alloc size then only allocate what's needed.
	// Once it goes over the allocation size then allocate in chunks.
	if db.datasz < db.AllocSize {
		sz = db.datasz
	} else {
		sz += db.AllocSize
	}

	// Truncate and fsync to ensure file size metadata is flushed.
	// https://github.com/boltdb/bolt/issues/284
	if !db.NoGrowSync && !db.readOnly {
		if runtime.GOOS != "windows" {
			if err := db.file.Truncate(int64(sz)); err != nil {
				return fmt.Errorf("file resize error: %s", err)
			}
		}
		if err := db.file.Sync(); err != nil {
			return fmt.Errorf("file sync error: %s", err)
		}
	}

	db.filesz = sz
	return nil
}

// write writes any dirty pages to disk.
func (db *DB) write() error {
	// Sort pages by id.
	// 保证写的页是有序的
	pages := make(pages, 0, len(db.pages))
	for _, p := range db.pages {
		pages = append(pages, p)
	}
	// Clear out page cache early.
	db.pages = make(map[pgid]*page)
	sort.Sort(pages)

	// Write pages to disk in order.
	for _, p := range pages {
		// 页数和偏移量
		size := (int(p.overflow) + 1) * db.pageSize
		offset := int64(p.id) * int64(db.pageSize)

		// Write out page in "max allocation" sized chunks.
		ptr := (*[maxAllocSize]byte)(unsafe.Pointer(p))
		// 循环写某一页
		for {
			// Limit our write to our max allocation size.
			sz := size
			// 2^31=2G
			if sz > maxAllocSize-1 {
				sz = maxAllocSize - 1
			}

			// Write chunk to disk.
			buf := ptr[:sz]
			if _, err := db.ops.writeAt(buf, offset); err != nil {
				return err
			}

			// Update statistics.
			//tx.stats.Write++

			// Exit inner for loop if we've written all the chunks.
			size -= sz
			if size == 0 {
				break
			}

			// Otherwise move offset forward and move pointer to next chunk.
			// 移动偏移量
			offset += int64(sz)
			// 同时指针也移动
			ptr = (*[maxAllocSize]byte)(unsafe.Pointer(&ptr[sz]))
		}
	}

	// Ignore file sync if flag is set on DB.
	if !db.NoSync || IgnoreNoSync {
		if err := fdatasync(db); err != nil {
			return err
		}
	}

	// Put small pages back to page pool.
	for _, p := range pages {
		// Ignore page sizes over 1 page.
		// These are allocated using make() instead of the page pool.
		if int(p.overflow) != 0 {
			continue
		}

		buf := (*[maxAllocSize]byte)(unsafe.Pointer(p))[:db.pageSize]

		// See https://go.googlesource.com/go/+/f03c9202c43e0abb130669852082117ca50aa9b1
		// 清空buf，然后放入pagePool中
		for i := range buf {
			buf[i] = 0
		}
		db.pagePool.Put(buf)
	}

	return nil
}

// Check performs several consistency checks on the database for this transaction.
// An error is returned if any inconsistency is found.
//
// It can be safely run concurrently on a writable transaction. However, this
// incurs a high cost for large databases and databases with a lot of subbuckets
// because of caching. This overhead can be removed if running on a read-only
// transaction, however, it is not safe to execute other writer transactions at
// the same time.
func (db *DB) Check() <-chan error {
	ch := make(chan error)
	go db.check(ch)
	return ch
}

func (db *DB) check(ch chan error) {
	// Check if any pages are double freed.
	freed := make(map[pgid]bool)
	all := make([]pgid, db.freelist.count())
	db.freelist.copyall(all)
	for _, id := range all {
		if freed[id] {
			ch <- fmt.Errorf("page %d: already freed", id)
		}
		freed[id] = true
	}

	// Track every reachable page.
	reachable := make(map[pgid]*page)
	reachable[0] = db.pageInfo(0) // meta0
	reachable[1] = db.pageInfo(1) // meta1
	for i := uint32(0); i <= db.pageInfo(db.usedMeta.freelist).overflow; i++ {
		reachable[db.usedMeta.freelist+pgid(i)] = db.pageInfo(db.usedMeta.freelist)
	}

	// Recursively check buckets.
	db.checkBucket(db.root, reachable, freed, ch)

	// Ensure all pages below high water mark are either reachable or freed.
	for i := pgid(0); i < db.usedMeta.pgid; i++ {
		_, isReachable := reachable[i]
		if !isReachable && !freed[i] {
			ch <- fmt.Errorf("page %d: unreachable unfreed", int(i))
		}
	}

	// Close the channel to signal completion.
	close(ch)
}

func (db *DB) checkBucket(b *Bucket, reachable map[pgid]*page, freed map[pgid]bool, ch chan error) {
	// Ignore inline buckets.
	if b.root == 0 {
		return
	}

	// Check every page used by this bucket.
	b.db.forEachPage(b.root, 0, func(p *page, _ int) {
		if p.id > db.usedMeta.pgid {
			ch <- fmt.Errorf("page %d: out of bounds: %d", int(p.id), int(b.db.usedMeta.pgid))
		}

		// Ensure each page is only referenced once.
		for i := pgid(0); i <= pgid(p.overflow); i++ {
			var id = p.id + i
			if _, ok := reachable[id]; ok {
				ch <- fmt.Errorf("page %d: multiple references", int(id))
			}
			reachable[id] = p
		}

		// We should only encounter un-freed leaf and branch pages.
		if freed[p.id] {
			ch <- fmt.Errorf("page %d: reachable freed", int(p.id))
		} else if (p.flags&branchPageFlag) == 0 && (p.flags&leafPageFlag) == 0 {
			ch <- fmt.Errorf("page %d: invalid type: %s", int(p.id), p.typ())
		}
	})

	// Check each bucket within this bucket.
	_ = b.ForEach(func(k, v []byte) error {
		if child := b.Bucket(k); child != nil {
			db.checkBucket(child, reachable, freed, ch)
		}
		return nil
	})
}

// forEachPage iterates over every page within a given page and executes a function.
func (db *DB) forEachPage(pgid pgid, depth int, fn func(*page, int)) {
	p := db.pageInfo(pgid)

	// Execute function.
	fn(p, depth)

	// Recursively loop over children.
	if (p.flags & branchPageFlag) != 0 {
		for i := 0; i < int(p.count); i++ {
			elem := p.branchPageElement(uint16(i))
			db.forEachPage(elem.pgid, depth+1, fn)
		}
	}
}

// writeMeta writes the meta to the disk.
func (db *DB) writeMeta() error {
	// Create a temporary buffer for the meta page.
	buf := make([]byte, db.pageSize)
	p := db.pageInBuffer(buf, 0)
	// 将事务的元信息写入到页中
	db.usedMeta.write(p)

	// Write the meta page to file.
	if _, err := db.ops.writeAt(buf, int64(p.id)*int64(db.pageSize)); err != nil {
		return err
	}
	if !db.NoSync || IgnoreNoSync {
		if err := fdatasync(db); err != nil {
			return err
		}
	}

	// Update statistics.
	//tx.stats.Write++

	return nil
}

// _assert will panic with a given formatted message if the given condition is false.
func _assert(condition bool, msg string, v ...interface{}) {
	if !condition {
		panic(fmt.Sprintf("assertion failed: "+msg, v...))
	}
}
