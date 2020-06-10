package txDB.concurrency;

import java.util.HashMap;
import java.util.concurrent.atomic.AtomicInteger;

import txDB.Config;
import txDB.recovery.LogManager;
import txDB.recovery.LogRecord;

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

    public Transaction begin() throws InterruptedException {
        Transaction txn = new Transaction(this.nextTxnId.getAndIncrement());
        if (Config.ENABLE_LOGGING) {
            // TODO
            LogRecord logRecord = new LogRecord(txn.getTxnId(), LogRecord.LogRecordType.BEGIN);
            logManager.appendLogRecord(logRecord);
        }
        txnMap.put(txn.getTxnId(), txn);
        return txn;
    }

    public void commit(Transaction txn) throws InterruptedException {
        // TODO
        txn.setTransactionState(Transaction.TransactionState.COMMITTED);

        if (Config.ENABLE_LOGGING) {
            // TODO
            LogRecord logRecord = new LogRecord(txn.getTxnId(), LogRecord.LogRecordType.COMMIT);
            logManager.appendLogRecord(logRecord);
        }
    }

    public void abort(Transaction txn) throws InterruptedException {
        // TODO
        txn.setTransactionState(Transaction.TransactionState.ABORTED);

        if (Config.ENABLE_LOGGING) {
            // TODO
            LogRecord logRecord = new LogRecord(txn.getTxnId(), LogRecord.LogRecordType.COMMIT);
            logManager.appendLogRecord(logRecord);
        }
    }

    public Transaction getTransaction(int txnId) {
        if (txnMap.containsKey(txnId)) {
            return txnMap.get(txnId);
        }

        return null;
    }
}
