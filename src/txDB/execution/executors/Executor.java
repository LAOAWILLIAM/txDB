package txDB.execution.executors;

import txDB.buffer.BufferManager;
import txDB.concurrency.LockManager;
import txDB.concurrency.Transaction;
import txDB.recovery.LogManager;
import txDB.storage.disk.DiskManager;

public class Executor {
    // TODO
    protected DiskManager diskManager;
    protected BufferManager bufferManager;
    protected LockManager lockManager;
    protected LogManager logManager;
    protected Transaction txn;

    public Executor(DiskManager diskManager,
                    BufferManager bufferManager,
                    LockManager lockManager,
                    LogManager logManager,
                    Transaction txn) {
        this.diskManager = diskManager;
        this.bufferManager = bufferManager;
        this.lockManager = lockManager;
        this.logManager = logManager;
        this.txn = txn;
    }
}
