package txDB.recovery;

import txDB.concurrency.Transaction;
import txDB.storage.page.Page;

import java.io.Serializable;
import java.util.HashMap;

public class ActiveTxnsAndDirtyPages implements Serializable {
    private HashMap<Integer, Transaction.TransactionState> activeTxnMap;
    private HashMap<Integer, Integer> dirtyPageMap;

    public ActiveTxnsAndDirtyPages(HashMap<Integer, Transaction.TransactionState> activeTxnMap, HashMap<Integer, Integer> dirtyPageMap) {
        this.activeTxnMap = activeTxnMap;
        this.dirtyPageMap = dirtyPageMap;
    }

    public HashMap<Integer, Transaction.TransactionState> getActiveTxnMap() {
        return activeTxnMap;
    }

    public HashMap<Integer, Integer> getDirtyPageMap() {
        return dirtyPageMap;
    }
}
