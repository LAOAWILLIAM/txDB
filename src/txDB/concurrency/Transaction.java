package txDB.concurrency;

import txDB.Config;
import txDB.storage.page.Page;
import txDB.storage.table.RecordID;
import txDB.storage.table.Table;
import txDB.storage.table.Tuple;

import java.util.*;

public class Transaction {
    // TODO
    public enum TransactionState {GROWING, SHRINKING, COMMITTED, ABORTED, RESTARTED}
    public enum WriteType {INSERT, UPDATE, DELETE}
    private int txnId;
    private int prevLsn;
    private TransactionState transactionState;
    private Queue<Page> indexPageQueue;         // index page latched
    private HashSet<RecordID> sharedLockSet;    // tuple shared locked
    private HashSet<RecordID> exclusiveLockSet; // tuple exclusive locked
    private Queue<WriteRecord> writeRecordQueue;

    public Transaction(int txnId) {
        this.txnId = txnId;
        this.prevLsn = Config.INVALID_LSN;
        this.transactionState = TransactionState.GROWING;
        this.indexPageQueue = new LinkedList<>();
        this.sharedLockSet = new HashSet<>();
        this.exclusiveLockSet = new HashSet<>();
        this.writeRecordQueue = new LinkedList<>();
    }

    public class WriteRecord {
        private RecordID recordID;
        private WriteType writeType;
        private Table table;
        private Tuple oldTuple;
        private Tuple newTuple;

        public WriteRecord(RecordID recordID, WriteType writeType, Table table, Tuple oldTuple, Tuple newTuple) {
            this.recordID = recordID;
            this.writeType = writeType;
            this.table = table;
            this.oldTuple = oldTuple;
            this.newTuple = newTuple;
        }

        public RecordID getRecordID() {
            return recordID;
        }

        public Table getTable() {
            return table;
        }

        public Tuple getOldTuple() {
            return oldTuple;
        }

        public Tuple getNewTuple() {
            return newTuple;
        }

        public WriteType getWriteType() {
            return writeType;
        }
    }

    public int getTxnId() {
        return txnId;
    }

    public TransactionState getTransactionState() {
        return transactionState;
    }

    public void setTransactionState(TransactionState transactionState) {
        this.transactionState = transactionState;
    }

    public Queue<Page> getIndexPageQueue() {
        return indexPageQueue;
    }

    public void pushIndexPageQueue(Page page) {
        indexPageQueue.add(page);
    }

    public Page popIndexPageQueue() {
        return indexPageQueue.poll();
    }

    public HashSet<RecordID> getSharedLockSet() {
        return sharedLockSet;
    }

    public HashSet<RecordID> getExclusiveLockSet() {
        return exclusiveLockSet;
    }

    public boolean isRecordSharedLocked(RecordID recordID) {
        return sharedLockSet.contains(recordID);
    }

    public boolean isRecordExclusiveLocked(RecordID recordID) {
        return exclusiveLockSet.contains(recordID);
    }

    public int getPrevLsn() {
        return prevLsn;
    }

    public void setPrevLsn(int prevLsn) {
        this.prevLsn = prevLsn;
    }

    public Queue<WriteRecord> getWriteRecordQueue() {
        return writeRecordQueue;
    }

    public void pushWriteRecordQueue(WriteRecord writeRecord) {
        writeRecordQueue.add(writeRecord);
    }

    public WriteRecord popWriteRecordQueue() {
        return writeRecordQueue.poll();
    }
}
