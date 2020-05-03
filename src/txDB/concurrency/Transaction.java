package txDB.concurrency;

public class Transaction {
    // TODO
    private int txnId;

    public Transaction(int txnId) {
        this.txnId = txnId;
    }

    public int getTxnId() {
        return txnId;
    }
}
