package txDB.concurrency;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicInteger;

import txDB.Config;
import txDB.recovery.LogManager;
import txDB.recovery.LogRecord;
import txDB.storage.table.RecordID;
import txDB.storage.table.Table;

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

    /**
     *
     * @return
     * @throws InterruptedException
     */
    public Transaction begin() throws InterruptedException {
        Transaction txn = new Transaction(this.nextTxnId.getAndIncrement());
        if (Config.ENABLE_LOGGING) {
            // TODO
            LogRecord logRecord = new LogRecord(txn.getPrevLsn(), txn.getTxnId(), LogRecord.LogRecordType.BEGIN);
            int lsn = logManager.appendLogRecord(logRecord, false);
            txn.setPrevLsn(lsn);
        }
        txnMap.put(txn.getTxnId(), txn);
        return txn;
    }

    /**
     *
     * @param txn
     * @throws InterruptedException
     */
    public void commit(Transaction txn) throws InterruptedException {
        // TODO
        txn.setTransactionState(Transaction.TransactionState.COMMITTED);

        if (Config.ENABLE_LOGGING) {
            LogRecord logRecord = new LogRecord(txn.getPrevLsn(), txn.getTxnId(), LogRecord.LogRecordType.COMMIT);
            int lsn = logManager.appendLogRecord(logRecord, true);
            txn.setPrevLsn(lsn);
        }

        releaseAllLocks(txn);
    }

    /**
     * just clarify here: Abort == Rollback
     * @param txn
     * @throws InterruptedException
     */
    public void abort(Transaction txn) throws InterruptedException {
        // TODO
        txn.setTransactionState(Transaction.TransactionState.ABORTED);

        // Rollback
        Queue<Transaction.WriteRecord> writeRecords = txn.getWriteRecordQueue();
        Transaction.WriteRecord writeRecord;
        Table table;
        while (!writeRecords.isEmpty()) {
            writeRecord = txn.popWriteRecordQueue();
            table = writeRecord.getTable();
            if (writeRecord.getWriteType() == Transaction.WriteType.UPDATE) {
//                System.out.println("rollback update");
                table.updateTuple(writeRecord.getOldTuple(), writeRecord.getRecordID(), txn);
            } else if (writeRecord.getWriteType() == Transaction.WriteType.INSERT) {
                // TODO
            } else if (writeRecord.getWriteType() == Transaction.WriteType.DELETE) {
                // TODO
            } else continue;
        }

        if (Config.ENABLE_LOGGING) {
            // TODO
            LogRecord logRecord = new LogRecord(txn.getPrevLsn(), txn.getTxnId(), LogRecord.LogRecordType.ABORT);
            int lsn = logManager.appendLogRecord(logRecord, true);
            txn.setPrevLsn(lsn);
        }

        releaseAllLocks(txn);
    }

    public Transaction getTransaction(int txnId) {
        if (txnMap.containsKey(txnId)) {
            return txnMap.get(txnId);
        }

        return null;
    }

    private void releaseAllLocks(Transaction txn) {
        HashSet<RecordID> recordIDHashSet = new HashSet<>();
        recordIDHashSet.addAll(txn.getSharedLockSet());
        recordIDHashSet.addAll(txn.getExclusiveLockSet());

        for (RecordID recordID : recordIDHashSet) {
            lockManager.unlock(txn, recordID);
        }
    }
}
