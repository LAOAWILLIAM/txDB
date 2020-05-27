package txDB.execution.executors;

import txDB.buffer.BufferManager;
import txDB.concurrency.LockManager;
import txDB.concurrency.Transaction;
import txDB.execution.plans.AggregationPlan;
import txDB.recovery.LogManager;
import txDB.storage.disk.DiskManager;

public class AggregationExecutor extends Executor {
    private AggregationPlan aggregationPlan;
    private Executor childExecutor;

    public AggregationExecutor(AggregationPlan aggregationPlan,
                               DiskManager diskManager,
                               BufferManager bufferManager,
                               LockManager lockManager,
                               LogManager logManager,
                               Transaction txn) {
        super(diskManager, bufferManager, lockManager, logManager, txn);
        this.aggregationPlan = aggregationPlan;
    }
}
