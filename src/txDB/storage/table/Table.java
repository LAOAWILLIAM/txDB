package txDB.storage.table;

import txDB.Config;
import txDB.buffer.BufferManager;
import txDB.concurrency.LockManager;
import txDB.concurrency.Transaction;
import txDB.recovery.LogManager;
import txDB.storage.page.TablePage;
import txDB.storage.page.Page;
import txDB.concurrency.Transaction.TransactionState;

public class Table {
    // TODO
    private BufferManager bufferManager;
    private LockManager lockManager;
    private LogManager logManager;
    private int firstPageId;

    // first page exists
    public Table(BufferManager bufferManager, LockManager lockManager, LogManager logManager, int firstPageId) {
        this.bufferManager = bufferManager;
        this.lockManager = lockManager;
        this.logManager = logManager;
        this.firstPageId = firstPageId;
    }

    // first page does not exist
    public Table(BufferManager bufferManager, LockManager lockManager, LogManager logManager, Transaction txn) {
        this.bufferManager = bufferManager;
        this.lockManager = lockManager;
        this.logManager = logManager;

        Page firstPage = this.bufferManager.newPage();
        this.firstPageId = firstPage.getPageId();
        TablePage firstTablePage = new TablePage(firstPage);
        this.bufferManager.replacePage(firstTablePage);
        firstTablePage.initialize(firstPageId, Config.PAGE_SIZE, Config.INVALID_PAGE_ID, this.logManager, txn);
        this.bufferManager.unpinPage(firstPageId, true);
    }

    public int getFirstPageId() {
        return this.firstPageId;
    }

    public Tuple getTuple(RecordID recordID, Transaction txn) {
        Page page = bufferManager.fetchPage(recordID.getPageId());
        if (page == null) {
            // abort this transaction
            txn.setTransactionState(TransactionState.ABORTED);
            return null;
        }

        TablePage tablePage = new TablePage(page);
        tablePage.readLatch();
        bufferManager.replacePage(tablePage);
        Tuple res = tablePage.getTuple(recordID, txn, lockManager);
        tablePage.readUnlatch();
        bufferManager.unpinPage(recordID.getPageId(), false);
        return res;
    }

    public boolean insertTuple(Tuple tuple, RecordID recordID, Transaction txn) {
        if (tuple.getTupleSize() + 32 > Config.PAGE_SIZE) {
            // abort this transaction
            txn.setTransactionState(TransactionState.ABORTED);
            return false;
        }

        Page curPage = bufferManager.fetchPage(firstPageId);
        if (curPage == null) {
            // abort this transaction
            txn.setTransactionState(TransactionState.ABORTED);
            return false;
        }

        TablePage curTablePage = new TablePage(curPage);
        bufferManager.replacePage(curTablePage);

        curTablePage.writeLatch();
        TablePage newTablePage;
//        while (!curTablePage.insertTuple(tuple, recordID, txn, lockManager, logManager)) {
        while (curTablePage.insertTuple(tuple, txn, lockManager, logManager) == null) {
            int nextPageId = curTablePage.getNextPageId();
            if (nextPageId != Config.INVALID_PAGE_ID) {
                curTablePage.writeUnlatch();
                bufferManager.unpinPage(curTablePage.getPageId(), false);
                Page newPage = bufferManager.fetchPage(nextPageId);
                newPage.readLatch();
                newTablePage = new TablePage(newPage);
                newPage.readUnlatch();
                newTablePage.writeLatch();
                bufferManager.replacePage(newTablePage);
            } else {
                Page newPage = bufferManager.newPage();
                if (newPage == null) {
                    curTablePage.writeUnlatch();
                    bufferManager.unpinPage(curTablePage.getPageId(), false);
                    // abort this transaction
                    txn.setTransactionState(TransactionState.ABORTED);
                    return false;
                }
                newPage.readLatch();
//                System.out.println("table new page " + newPage.getPageId());
                newTablePage = new TablePage(newPage);
                newPage.readUnlatch();
                newTablePage.writeLatch();
                bufferManager.replacePage(newTablePage);
                curTablePage.setNextPageId(newTablePage.getPageId());
                newTablePage.initialize(newTablePage.getPageId(), Config.PAGE_SIZE, curTablePage.getPageId(), logManager, txn);
                curTablePage.writeUnlatch();
                bufferManager.unpinPage(curTablePage.getPageId(), true);
            }
            curTablePage = newTablePage;
        }
        firstPageId = curTablePage.getPageId();
        curTablePage.writeUnlatch();
//        System.out.println("curTablePage: " + curTablePage.getTablePageId());
        bufferManager.unpinPage(curTablePage.getPageId(), true);
        // transaction operation here
        txn.pushWriteRecordQueue(txn.new WriteRecord(recordID, Transaction.WriteType.INSERT, this, null, null));
        return true;
    }

    public RecordID insertTuple(Tuple tuple, Transaction txn) {
        if (tuple.getTupleSize() + 32 > Config.PAGE_SIZE) {
            // abort this transaction
            txn.setTransactionState(TransactionState.ABORTED);
            return null;
        }

        Page curPage = bufferManager.fetchPage(firstPageId);
        if (curPage == null) {
            // abort this transaction
            txn.setTransactionState(TransactionState.ABORTED);
            return null;
        }

        TablePage curTablePage = new TablePage(curPage);
        bufferManager.replacePage(curTablePage);

        curTablePage.writeLatch();
        TablePage newTablePage;
        RecordID recordID;
//        while (!curTablePage.insertTuple(tuple, recordID, txn, lockManager, logManager)) {
        while ((recordID = curTablePage.insertTuple(tuple, txn, lockManager, logManager)) == null) {
            int nextPageId = curTablePage.getNextPageId();
            if (nextPageId != Config.INVALID_PAGE_ID) {
                curTablePage.writeUnlatch();
                bufferManager.unpinPage(curTablePage.getPageId(), false);
                Page newPage = bufferManager.fetchPage(nextPageId);
                newPage.readLatch();
                newTablePage = new TablePage(newPage);
                newPage.readUnlatch();
                newTablePage.writeLatch();
                bufferManager.replacePage(newTablePage);
            } else {
                Page newPage = bufferManager.newPage();
                if (newPage == null) {
                    curTablePage.writeUnlatch();
                    bufferManager.unpinPage(curTablePage.getPageId(), false);
                    // abort this transaction
                    txn.setTransactionState(TransactionState.ABORTED);
                    return null;
                }
                newPage.readLatch();
//                System.out.println("table new page " + newPage.getPageId());
                newTablePage = new TablePage(newPage);
                newPage.readUnlatch();
                newTablePage.writeLatch();
                bufferManager.replacePage(newTablePage);
                curTablePage.setNextPageId(newTablePage.getPageId());
                newTablePage.initialize(newTablePage.getPageId(), Config.PAGE_SIZE, curTablePage.getPageId(), logManager, txn);
                curTablePage.writeUnlatch();
                bufferManager.unpinPage(curTablePage.getPageId(), true);
            }
            curTablePage = newTablePage;
        }
        firstPageId = curTablePage.getPageId();
        curTablePage.writeUnlatch();
//        System.out.println("curTablePage: " + curTablePage.getTablePageId());
        bufferManager.unpinPage(curTablePage.getPageId(), true);
        // transaction operation here
        txn.pushWriteRecordQueue(txn.new WriteRecord(recordID, Transaction.WriteType.INSERT, this, null, null));
        return recordID;
    }

    public boolean updateTuple(Tuple newTuple, RecordID recordID, Transaction txn) {
        // TODO
        Page page = bufferManager.fetchPage(recordID.getPageId());
        if (page == null) {
            // abort this transaction
            txn.setTransactionState(TransactionState.ABORTED);
            return false;
        }

        TablePage tablePage = new TablePage(page);
        tablePage.writeLatch();
        bufferManager.replacePage(tablePage);
        Tuple oldTuple = new Tuple();
        boolean res = tablePage.updateTuple(newTuple, oldTuple, recordID, txn, lockManager, logManager);
        tablePage.writeUnlatch();
        bufferManager.unpinPage(recordID.getPageId(), res);

        if (res && txn.getTransactionState() != TransactionState.ABORTED) {
            txn.pushWriteRecordQueue(txn.new WriteRecord(recordID, Transaction.WriteType.UPDATE, this, oldTuple, newTuple));
        }

        return res;
    }
}
