package txDB.execution.executors;

import txDB.buffer.BufferManager;
import txDB.concurrency.LockManager;
import txDB.concurrency.Transaction;
import txDB.execution.plans.InsertPlan;
import txDB.recovery.LogManager;
import txDB.storage.disk.DiskManager;
import txDB.storage.table.Tuple;

public class InsertExecutor extends Executor {
    // TODO
    private InsertPlan insertPlan;

    public InsertExecutor(InsertPlan insertPlan,
                          DiskManager diskManager,
                          BufferManager bufferManager,
                          LockManager lockManager,
                          LogManager logManager,
                          Transaction txn) {
        super(diskManager, bufferManager, lockManager, logManager, txn);
        this.insertPlan = insertPlan;
    }

    public Tuple next() {
        return null;
    }
}
