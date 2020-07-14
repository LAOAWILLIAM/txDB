# txDB
A on-disk database system built from scratch

* Thread safe in concurrent environment;
* Support insertion, delete, update and selection;
* Support LRU buffer pool;
* Support both In-memory and on-disk B+ tree index;
* Support query plan and execution
* Support transaction concurrency control using 2PL;
* Support runtime rollback, crash recovery using persisted log and checkpoint;
* Support Variable length text;
* Internal parameter configurable;
