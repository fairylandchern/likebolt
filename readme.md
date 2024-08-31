# bolt简洁

*bolt是go语言写的一个基于b+树的kv数据库，简单易使用*

# 相关概念图
![page](https://github.com/user-attachments/assets/8072d7ee-46e2-4ece-bd2f-bf10e2f45aac)


*如图，展示了page相关的定义与结构*
![bucket](https://github.com/user-attachments/assets/a5e723a3-0abf-4c7a-89e4-e8ee352f1d1b)


*如图，展示了bolt的底层存储结构，bucket就是一颗b+树，然后基于其的结构图，很好的介绍了b+树的结构*

# 对代码的简单理解（结合测试用例理解）

    func TestDB_Update(t *testing.T) {
        db := MustOpenDB()
        defer db.MustClose()
        // update操作，里面封装了put（增），delete（删）数据之后操作的封装，
        // tx.Commit()提交数据并且进行b+树的调整（合并、删除节点）并同步到文件中
        if err := db.Update(func(tx *bolt.Tx) error {
           b, err := tx.CreateBucket([]byte("widgets"))
           if err != nil {
              t.Fatal(err)
           }
           if err := b.Put([]byte("foo"), []byte("bar")); err != nil {
              t.Fatal(err)
           }
           if err := b.Put([]byte("baz"), []byte("bat")); err != nil {
              t.Fatal(err)
           }
           if err := b.Delete([]byte("foo")); err != nil {
              t.Fatal(err)
           }
           return nil
        }); err != nil {
           t.Fatal(err)
        }
        if err := db.View(func(tx *bolt.Tx) error {
           b := tx.Bucket([]byte("widgets"))
           if v := b.Get([]byte("foo")); v != nil {
              t.Fatalf("expected nil value, got: %v", v)
           }
           if v := b.Get([]byte("baz")); !bytes.Equal(v, []byte("bat")) {
              t.Fatalf("unexpected value: %v", v)
           }
           return nil
        }); err != nil {
           t.Fatal(err)
        }
    }

*update操作，里面封装了put（增），delete（删）数据之后操作的封装，tx.Commit()提交数据并且进行b+树的调整（合并、删除节点）并同步到文件中*

# 总结

*阅读源码的时候建议结合测试用例一起进行，然后理解整个bolt的功能就是增删改查，其他就是辅助性的，比如数据的落盘、树的大小调整、页大小的调整等等*
