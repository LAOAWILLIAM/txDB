package txDB.execution.executors;

import txDB.buffer.BufferManager;
import txDB.concurrency.LockManager;
import txDB.concurrency.Transaction;
import txDB.execution.plans.*;
import txDB.recovery.LogManager;
import txDB.storage.disk.DiskManager;
import txDB.storage.table.Tuple;

public class Executor {
    // TODO: Parallel access methods shall be introduced, e.g., current overhead of sequential scan is high !!!
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

    protected Tuple next() {
        return null;
    }

    protected Executor newExecutor(Plan plan) {
        if (plan.getPlanType() == Plan.planType.SEQSCAN) {
            return new SeqScanExecutor((SeqScanPlan) plan, diskManager, bufferManager, lockManager, null, txn);
        } else if (plan.getPlanType() == Plan.planType.PREDEVAL) {
            return new PredEvalExecutor((PredEvalPlan) plan, diskManager, bufferManager, lockManager, null, txn);
        } else if (plan.getPlanType() == Plan.planType.INSERT) {
            return new InsertExecutor((InsertPlan) plan, diskManager, bufferManager, lockManager, null, txn);
        } else if (plan.getPlanType() == Plan.planType.AGGREGATION) {
            return new AggregationExecutor((AggregationPlan) plan, diskManager, bufferManager, lockManager, null, txn);
        } else if (plan.getPlanType() == Plan.planType.JOIN) {
            return new JoinExecutor((JoinPlan) plan, diskManager, bufferManager, lockManager, null, txn);
        }
        throw new RuntimeException("Unsupported query plan type.");
    }
}
