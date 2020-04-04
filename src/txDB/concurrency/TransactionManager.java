package txDB.concurrency;

import txDB.recovery.LogManager;

public class TransactionManager {
    // TODO
    private LockManager lockManager;
    private LogManager logManager;

    public TransactionManager(LockManager lockManager, LogManager logManager) {
        this.lockManager = lockManager;
        this.logManager = logManager;
    }

    public Transaction begin() {
        // TODO
        return null;
    }

    public void commit(Transaction txn) {
        // TODO
    }

    public void abort(Transaction txn) {
        // TODO
    }


}
