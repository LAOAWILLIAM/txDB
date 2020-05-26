package txDB.concurrency;

import txDB.storage.page.Page;
import txDB.storage.table.RecordID;

import java.util.HashSet;
import java.util.LinkedList;
import java.util.Queue;

public class Transaction {
    // TODO
    public enum TransactionState {GROWING, SHRINKING, COMMITTED, ABORTED, RESTARTED}
    private int txnId;
    private TransactionState transactionState;
    private Queue<Page> indexPageQueue;         // index page latched
    private HashSet<RecordID> sharedLockSet;    // tuple shared locked
    private HashSet<RecordID> exclusiveLockSet; // tuple exclusive locked

    public Transaction(int txnId) {
        this.txnId = txnId;
        this.transactionState = TransactionState.GROWING;
        this.indexPageQueue = new LinkedList<>();
        this.sharedLockSet = new HashSet<>();
        this.exclusiveLockSet = new HashSet<>();
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
}
