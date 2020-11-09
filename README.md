# txDB
An on-disk relational database system built from scratch

* Thread safe in concurrent environment;
* Support insertion, update and selection (delete in progress);
* Support LRU buffer pool;
* Support both In-memory and on-disk B+ tree index;
* Support query plan and execution
* Support transaction concurrency control using SS2PL, deadlock detection and prevention;
* Support runtime rollback, AREIS crash recovery (in progress) using persisted log and checkpoint;
* Support Variable length text;
* Internal parameter configurable;
