# bolt简介
*boltdb是基于go实现的、开源的、基于b+树实现的一个kv数据库*
# 删减版代码块地址
https://github.com/fairylandchern/likebolt
# 测试用例
```
func TestDB_Put(t *testing.T) {
    db := InitDB("./db.txt")
    if db == nil {
       panic("err db nil")
    }
    err := db.Put([]byte("test"), []byte("123yuguangxing"))
    if err != nil {
       fmt.Println("err put data:", err)
       return
    }
    val := db.Get([]byte("test"))
    fmt.Println("get data:", string(val))
    err = db.Commit()
    if err != nil {
       fmt.Println("err commit:", err)
    }
}
```
# 输出结果如下
```
=== RUN   TestDB_Put
get data: 123yuguangxing
reblance tm: 900ns
spill time: 9.882µs
write time: 43.335659ms
--- PASS: TestDB_Put (0.04s)
PASS
```
# 功能简介
*db.Get()查询数据，db.Put()增改数据，db.Delete()删除数据,db.Commit()用于增、删、改数据之后的后置操作，对b+树及页进行调整操作，详细细节可以参阅代码*
*阅读代码的方法：主体逻辑都在db.go文件中，其他文件都是基于增、删、改、查主体逻辑的扩展*