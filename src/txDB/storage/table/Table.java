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
    public Table(BufferManager bufferManager, LockManager lockManager, LogManager logManager) {
        this.bufferManager = bufferManager;
        this.lockManager = lockManager;
        this.logManager = logManager;
    }

    public boolean insertTuple(Tuple tuple, RecordID recordID, Transaction txn) {
        if (tuple.getTupleSize() + 32 > Config.PAGE_SIZE) {
            // abort this transaction
            return false;
        }

        Page curPage = bufferManager.fetchPage(firstPageId);
        if (curPage == null) {
            // abort this transaction
            // TODO
            return false;
        }

        if (curPage instanceof TablePage) {
            while (!((TablePage) curPage).insertTuple(tuple, recordID, txn, lockManager, logManager)) {
                int nextPageId = ((TablePage) curPage).getNextPageId();
                if (nextPageId != Config.INVALID_PAGE_ID) {
                    bufferManager.unpinPage(curPage.getPageId(), false);
                    curPage = bufferManager.fetchPage(nextPageId);
                } else {
                    Page newPage = bufferManager.newPage();
                    if (newPage == null) {
                        bufferManager.unpinPage(curPage.getPageId(), false);
                        // abort this transaction
                        // TODO
                        return false;
                    }
                    ((TablePage) curPage).setNextPageId(nextPageId);
                    if (newPage instanceof TablePage) {
                        ((TablePage) newPage).setPrevPageId(curPage.getPageId());
                        ((TablePage) newPage).setNextPageId(Config.INVALID_PAGE_ID);
                        ((TablePage) newPage).setTupleCount(0);
                        ((TablePage) newPage).setFreeSpacePointer(Config.PAGE_SIZE);
                        bufferManager.unpinPage(curPage.getPageId(), true);
                        curPage = newPage;
                    }
                }
            }
            bufferManager.unpinPage(curPage.getPageId(), true);
            // transaction operation here TODO
            return true;
        }

        return false;
    }

    public Tuple getTuple(RecordID recordID, Transaction txn) {
        Page page = bufferManager.fetchPage(recordID.getPageId());
        if (page == null) {
            // abort this transaction
            // TODO
            return null;
        }

        if (page instanceof TablePage) {
            TablePage tablePage = (TablePage) page;
            Tuple res = tablePage.getTuple(recordID, txn, lockManager);
            bufferManager.unpinPage(recordID.getPageId(), false);
            return res;
        }

        return null;
    }
}
