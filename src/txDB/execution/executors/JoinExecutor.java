package txDB.execution.executors;

import txDB.buffer.BufferManager;
import txDB.concurrency.LockManager;
import txDB.concurrency.Transaction;
import txDB.execution.plans.JoinPlan;
import txDB.recovery.LogManager;
import txDB.storage.disk.DiskManager;

/**
 * I use Hash Join here
 */
public class JoinExecutor extends Executor {
    private JoinPlan joinPlan;
    private Executor childExecutor;

    public JoinExecutor(JoinPlan joinPlan,
                        DiskManager diskManager,
                        BufferManager bufferManager,
                        LockManager lockManager,
                        LogManager logManager,
                        Transaction txn) {
        super(diskManager, bufferManager, lockManager, logManager, txn);
        this.joinPlan = joinPlan;
    }
}
