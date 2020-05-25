package txDB.concurrency;

import txDB.storage.page.Page;

import java.util.LinkedList;
import java.util.Queue;

public class Transaction {
    // TODO
    public enum TransactionState {GROWING, SHRINKING, COMMITTED, ABORTED, RESTARTED}
    private int txnId;
    private TransactionState transactionState;
    private Queue<Page> indexPageQueue;

    public Transaction(int txnId) {
        this.txnId = txnId;
        this.transactionState = TransactionState.GROWING;
        this.indexPageQueue = new LinkedList<>();
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
}
