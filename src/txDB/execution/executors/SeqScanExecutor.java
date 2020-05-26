package txDB.execution.executors;

import txDB.Config;
import txDB.buffer.BufferManager;
import txDB.concurrency.LockManager;
import txDB.concurrency.Transaction;
import txDB.execution.plans.SeqScanPlan;
import txDB.recovery.LogManager;
import txDB.storage.disk.DiskManager;
import txDB.storage.page.MetaDataPage;
import txDB.storage.page.Page;
import txDB.storage.page.TablePage;
import txDB.storage.table.RecordID;
import txDB.storage.table.Table;
import txDB.storage.table.Tuple;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;

/**
 * It is a sequential scan executor
 */
public class SeqScanExecutor extends Executor {
    // TODO
    private SeqScanPlan seqScanPlan;
    private Table table;
    private int curIndex;
    private int tupleCount;
    private int curPageId;
    private int curCount;
    private int nextPageId;

    public SeqScanExecutor(SeqScanPlan seqScanPlan,
                           DiskManager diskManager,
                           BufferManager bufferManager,
                           LockManager lockManager,
                           LogManager logManager,
                           Transaction txn) {
        super(diskManager, bufferManager, lockManager, logManager, txn);
        this.seqScanPlan = seqScanPlan;
        this.initialize();
    }

    public void initialize() {
        Page page0 = bufferManager.fetchPage(0);
        try {
            ByteArrayInputStream bis = new ByteArrayInputStream(page0.getPageData());
            ObjectInputStream in = new ObjectInputStream(bis);
            MetaDataPage metaDataPage = (MetaDataPage) in.readObject();

            table = new Table(bufferManager, lockManager, logManager,
                    metaDataPage.getRelationMetaData(seqScanPlan.getRelationName()).getRootRelationPageId());

            curPageId = table.getFirstPageId();
            curIndex = 0;
            curCount = 0;

            Page page = bufferManager.fetchPage(curPageId);
            if (page == null) {
                // abort this transaction
                txn.setTransactionState(Transaction.TransactionState.ABORTED);
                return;
            }

            TablePage tablePage = new TablePage(page);
            tablePage.readLatch();
            bufferManager.replacePage(tablePage);
            tupleCount = tablePage.getTupleCount();
            nextPageId = tablePage.getNextPageId();
//            System.out.println("tupleCount: " + tupleCount + ", nextPageId: " + nextPageId);
            tablePage.readUnlatch();

        } catch (IOException | ClassNotFoundException e) {
            e.printStackTrace();
        }
    }

    public Tuple next() throws InterruptedException {
//        System.out.println("curCount: " + curCount);
        if (curCount == tupleCount) {
            bufferManager.unpinPage(curPageId, false);

            Page page = bufferManager.fetchPage(nextPageId);
            if (page == null) {
                // abort this transaction
                txn.setTransactionState(Transaction.TransactionState.ABORTED);
                return null;
            }

            TablePage tablePage = new TablePage(page);
            tablePage.readLatch();
            bufferManager.replacePage(tablePage);
            tupleCount = tablePage.getTupleCount();
            curPageId = nextPageId;
            nextPageId = tablePage.getNextPageId();
//            System.out.println("tupleCount: " + tupleCount + ", nextPageId: " + nextPageId);
            if (nextPageId == Config.INVALID_PAGE_ID) {
                tablePage.readUnlatch();
                return null;
            }
            curIndex = 0;
            curCount = 0;
            tablePage.readUnlatch();
        }

        RecordID recordID = new RecordID(curPageId, curIndex);
        curCount++;
        curIndex++;
        Tuple res = table.getTuple(recordID, txn);
        // TODO: unlock temporarily lie here
        lockManager.unlock(txn, recordID);
        return res;
    }
}
