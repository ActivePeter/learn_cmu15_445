之前在做pingcap talent plan 的 lab5，发现一个关于数据库何时返回结果的问题（写入内存后就返回还是持久化到磁盘后返回）
然后问了下群佬（冰冰的小冰），便让来学习cmu15_445的lab了

project0 是简单地cpp操作，矩阵运算细心点看就行了，比如row，col-》对应的下标就是rowi*col+coli

project1-1 LRU REPLACEMENT POLICY
实现lru replacer

- pin表示线程在使用，所以不能被回收，所以从中移出
- unpin表示最近使用完，加入队列
- 有新的要加入，或者扫描触发，Victim移出最旧的，

project1-2 BufferPoil
- Page Object (容器，装从硬盘加载出的数据)
    - page_id 代表哪一个物理page 
    - 无指向 INVALID_PAGE_ID
    - pin了这个page的thread的计数（pin了就不能free）
    - 是否dirty
    - 

