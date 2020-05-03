package txDB.storage.table;

import txDB.Config;
import txDB.buffer.BufferManager;
import txDB.concurrency.LockManager;
import txDB.concurrency.Transaction;
import txDB.recovery.LogManager;
import txDB.storage.page.TablePage;
import txDB.storage.page.Page;

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

    public boolean insertTuple(Tuple tuple, RecordID recordID, Transaction txn) {
        if (tuple.getTupleSize() + 32 > Config.PAGE_SIZE) {
//            System.out.println("bug 1");
            // abort this transaction
            return false;
        }

        Page curPage = bufferManager.fetchPage(firstPageId);
        if (curPage == null) {
//            System.out.println("bug 2");
            // abort this transaction
            // TODO
            return false;
        }

        TablePage curTablePage = new TablePage(curPage);
        bufferManager.replacePage(curTablePage);

        TablePage newTablePage;
        while (!curTablePage.insertTuple(tuple, recordID, txn, lockManager, logManager)) {
            int nextPageId = curTablePage.getNextPageId();
            if (nextPageId != Config.INVALID_PAGE_ID) {
                bufferManager.unpinPage(curTablePage.getPageId(), false);
                Page newPage = bufferManager.fetchPage(nextPageId);
                newTablePage = new TablePage(newPage);
                bufferManager.replacePage(newTablePage);
            } else {
                Page newPage = bufferManager.newPage();
                if (newPage == null) {
                    bufferManager.unpinPage(curTablePage.getPageId(), false);
                    // abort this transaction
                    // TODO
                    return false;
                }
//                System.out.println("table new page " + newPage.getPageId());
                newTablePage = new TablePage(newPage);
                bufferManager.replacePage(newTablePage);
                curTablePage.setNextPageId(newTablePage.getPageId());
                newTablePage.initialize(newTablePage.getPageId(), Config.PAGE_SIZE, curTablePage.getPageId(), logManager, txn);
                bufferManager.unpinPage(curTablePage.getPageId(), true);
            }
            curTablePage = newTablePage;
        }
        firstPageId = curTablePage.getPageId();
//        System.out.println("curTablePage: " + curTablePage.getTablePageId());
        bufferManager.unpinPage(curTablePage.getPageId(), true);
        // transaction operation here TODO
        return true;
    }

    public Tuple getTuple(RecordID recordID, Transaction txn) {
        Page page = bufferManager.fetchPage(recordID.getPageId());
        if (page == null) {
            // abort this transaction
            // TODO
            return null;
        }

        TablePage tablePage = new TablePage(page);
        bufferManager.replacePage(tablePage);
        Tuple res = tablePage.getTuple(recordID, txn, lockManager);
        bufferManager.unpinPage(recordID.getPageId(), false);
        return res;
    }
}
