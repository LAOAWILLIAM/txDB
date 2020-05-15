package txDB.concurrency;

import java.util.HashMap;
import java.util.concurrent.atomic.AtomicInteger;

import txDB.Config;
import txDB.recovery.LogManager;

public class TransactionManager {
    // TODO
    private LockManager lockManager;
    private LogManager logManager;
    private HashMap<Integer, Transaction> txnMap;
    private AtomicInteger nextTxnId;

    public TransactionManager(LockManager lockManager, LogManager logManager) {
        this.lockManager = lockManager;
        this.logManager = logManager;
        this.txnMap = new HashMap<>();
        this.nextTxnId = new AtomicInteger(0);
    }

    public Transaction begin() {
        Transaction txn = new Transaction(this.nextTxnId.getAndIncrement());
        if (Config.ENABLE_LOGGING) {
            // TODO
        }
        txnMap.put(txn.getTxnId(), txn);
        return txn;
    }

    public void commit(Transaction txn) {
        // TODO
        txn.setTransactionState(Transaction.TransactionState.COMMITTED);
    }

    public void abort(Transaction txn) {
        // TODO
        txn.setTransactionState(Transaction.TransactionState.ABORTED);
    }

    public Transaction getTransaction(int txnId) {
        if (txnMap.containsKey(txnId)) {
            return txnMap.get(txnId);
        }

        return null;
    }
}
