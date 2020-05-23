package txDB.concurrency;

public class Transaction {
    // TODO
    public enum TransactionState {GROWING, SHRINKING, COMMITTED, ABORTED, RESTARTED}
    private int txnId;
    private TransactionState transactionState;

    public Transaction(int txnId) {
        this.txnId = txnId;
        this.transactionState = TransactionState.GROWING;
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
}
