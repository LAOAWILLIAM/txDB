package test.storage.index;

import org.junit.Test;
import static org.junit.Assert.*;

import txDB.Config;
import txDB.buffer.BufferManager;
import txDB.concurrency.Transaction;
import txDB.concurrency.TransactionManager;
import txDB.recovery.LogManager;
import txDB.storage.disk.DiskManager;
import txDB.storage.index.BPlusTreeIndex;
import txDB.storage.index.BPlusTreeIndex;
import txDB.storage.page.BPlusTreeInnerPageNode;
import txDB.storage.page.BPlusTreeLeafPageNode;
import txDB.storage.page.BPlusTreePageNode;
import txDB.storage.page.Page;
import txDB.storage.table.Column;
import txDB.storage.table.RecordID;
import txDB.type.Type;

import java.io.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class BPlusTreeIndexTest {
    String dbName = "test";
    DiskManager diskManager = new DiskManager();
    LogManager logManager = new LogManager(diskManager);
    TransactionManager transactionManager = new TransactionManager(null, logManager);

    public BPlusTreeIndexTest() throws IOException {
        diskManager.dropFile(dbName);
        diskManager.createFile(dbName);
        diskManager.useFile(dbName);
    }

    @Test
    @SuppressWarnings("unchecked")
    public void serializableTest() throws InterruptedException {
        int bufferSize = 100;
        BufferManager bufferManager = new BufferManager(bufferSize, diskManager, logManager);

        Page page0 = bufferManager.newPage();
        assertNotNull(page0);
        assertEquals(page0.getPageId(), 0);

        try {
            BPlusTreePageNode<Float, Integer> bPlusTreePageNode = new BPlusTreePageNode<>();
            bPlusTreePageNode.setPageId(page0.getPageId());
            bPlusTreePageNode.setIndexPageType(BPlusTreePageNode.IndexPageType.LEAFPAGE);
            bPlusTreePageNode.getKeys().add(1f);
            bPlusTreePageNode.getKeys().add(2f);
            bPlusTreePageNode.getKeys().add(3f);

            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            ObjectOutput out = new ObjectOutputStream(bos);
            out.writeObject(bPlusTreePageNode);
            byte[] pageData = bos.toByteArray();
            System.out.println("pageData length: " + pageData.length);
            page0.setPageData(pageData);
            bufferManager.unpinPage(page0.getPageId(), true);
            bufferManager.flushPage(page0.getPageId());

            page0 = bufferManager.fetchPage(0);
            ByteArrayInputStream bis = new ByteArrayInputStream(page0.getPageData());
            ObjectInputStream in = new ObjectInputStream(bis);
            bPlusTreePageNode = (BPlusTreePageNode) in.readObject();
            assertEquals(bPlusTreePageNode.getPageId(), 0);
            assertEquals(bPlusTreePageNode.getIndexPageType(), BPlusTreePageNode.IndexPageType.LEAFPAGE);

        } catch (IOException | ClassNotFoundException e) {
            e.printStackTrace();
        }

        diskManager.close();
        diskManager.dropFile(dbName);
    }

    @Test
    @SuppressWarnings("unchecked")
    public void maxDegreeDetermineTest() {
        try {
            Column col0 = new Column("col0", Type.ColumnValueType.INTEGER, 4, 0);
            BPlusTreeInnerPageNode<Integer, RecordID> bPlusTreeInnerPageNode = new BPlusTreeInnerPageNode<>(-1, 1, 2, 0, 0, 100);

            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            ObjectOutput out = new ObjectOutputStream(bos);
            out.writeObject(bPlusTreeInnerPageNode);
            byte[] pageData = bos.toByteArray();
            System.out.println("pageData length: " + pageData.length);

            for (int i = 0; i < 300; i++) {
                bPlusTreeInnerPageNode.insertAndSort(i, 3);
                bos.reset();
                out = new ObjectOutputStream(bos);
                out.writeObject(bPlusTreeInnerPageNode);
                pageData = bos.toByteArray();
                System.out.println("pageData length: " + pageData.length);
            }

            BPlusTreeLeafPageNode<Integer, RecordID> bPlusTreeLeafPageNode = new BPlusTreeLeafPageNode<>(-1, new RecordID(7, 0), 1, 0, 100);
            bos.reset();
            out = new ObjectOutputStream(bos);
            out.writeObject(bPlusTreeLeafPageNode);
            pageData = bos.toByteArray();
            System.out.println("pageData length: " + pageData.length);

            for (int i = 0; i < 300; i++) {
                bPlusTreeLeafPageNode.insertAndSort(i, new RecordID(7, 1));
                bos.reset();
                out = new ObjectOutputStream(bos);
                out.writeObject(bPlusTreeLeafPageNode);
                pageData = bos.toByteArray();
                System.out.println("pageData length: " + pageData.length);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        diskManager.close();
        diskManager.dropFile(dbName);
    }

    @Test
    @SuppressWarnings("unchecked")
    public void rootAsLeafNodeTest() throws IOException, ClassNotFoundException, InterruptedException {
        int bufferSize = 100;
        BufferManager bufferManager = new BufferManager(bufferSize, diskManager, logManager);

        BPlusTreeIndex<Integer, Integer> bpti = new BPlusTreeIndex<>(bufferManager, Config.INVALID_PAGE_ID, 3, 3);
        Transaction txn0 = transactionManager.begin();
        bpti.insert(3, 6, txn0);

        try {
            Page page0 = bufferManager.fetchPage(0);
            ByteArrayInputStream bis = new ByteArrayInputStream(page0.getPageData());
            ObjectInputStream in = new ObjectInputStream(bis);
            BPlusTreePageNode<Integer, Integer> bPlusTreePageNode = (BPlusTreePageNode<Integer, Integer>) in.readObject();
            assertEquals(bPlusTreePageNode.getPageId(), 0);
            assertEquals(bPlusTreePageNode.getIndexPageType(), BPlusTreePageNode.IndexPageType.LEAFPAGE);
            assertEquals(bPlusTreePageNode.getKeys(), new ArrayList<>(Arrays.asList(3)));
            assertEquals(((BPlusTreeLeafPageNode<Integer, Integer>) bPlusTreePageNode).getValues(), new ArrayList<>(Arrays.asList(6)));
        } catch (IOException | ClassNotFoundException e) {
            e.printStackTrace();
        }

        diskManager.close();
        diskManager.dropFile(dbName);
    }

    @Test
    @SuppressWarnings("unchecked")
    public void rootAsLeafNodeFirstSplitTest() throws IOException, ClassNotFoundException, InterruptedException {
        int bufferSize = 100;
        BufferManager bufferManager = new BufferManager(bufferSize, diskManager, logManager);

        BPlusTreeIndex<Integer, Integer> bpti = new BPlusTreeIndex<>(bufferManager, Config.INVALID_PAGE_ID, 3, 3);
        Transaction txn0 = transactionManager.begin();

        bpti.insert(5, 100, txn0);
        bpti.insert(9, 101, txn0);
        bpti.insert(13, 102, txn0);
        assertEquals(bpti.find(5, txn0), new Integer(100));
        assertEquals(bpti.find(9, txn0), new Integer(101));
        assertEquals(bpti.find(13, txn0), new Integer(102));

        diskManager.close();
        diskManager.dropFile(dbName);
    }

    @Test
    public void rootAsLeafNodeFirstSplitAndInsertTest() throws IOException, ClassNotFoundException, InterruptedException {
        int bufferSize = 100;
        BufferManager bufferManager = new BufferManager(bufferSize, diskManager, logManager);

        BPlusTreeIndex<Integer, Integer> bpti = new BPlusTreeIndex<>(bufferManager, Config.INVALID_PAGE_ID, 3, 3);
        Transaction txn0 = transactionManager.begin();

        bpti.insert(5, 100, txn0);
        bpti.insert(9, 101, txn0);
        bpti.insert(13, 102, txn0);
        bpti.insert(20, 103, txn0);
        assertEquals(bpti.find(5, txn0), new Integer(100));
        assertEquals(bpti.find(9, txn0), new Integer(101));
        assertEquals(bpti.find(13, txn0), new Integer(102));
        assertEquals(bpti.find(20, txn0), new Integer(103));

        bpti.insert(1, 104, txn0);
        assertEquals(bpti.find(1, txn0), new Integer(104));

        bpti.insert(10, 105, txn0);
        assertEquals(bpti.find(10, txn0), new Integer(105));

        diskManager.close();
        diskManager.dropFile(dbName);
    }

    @Test
    @SuppressWarnings("unchecked")
    public void internalNodeSplitTest() throws IOException, ClassNotFoundException, InterruptedException {
        int bufferSize = 100;
        BufferManager bufferManager = new BufferManager(bufferSize, diskManager, logManager);

        BPlusTreeIndex<Integer, Integer> bpti = new BPlusTreeIndex<>(bufferManager, Config.INVALID_PAGE_ID, 3, 3);
        Transaction txn0 = transactionManager.begin();

        bpti.insert(5, 100, txn0);
        bpti.insert(9, 101, txn0);
        bpti.insert(13, 102, txn0);
        bpti.insert(20, 103, txn0);
        assertEquals(bpti.find(5, txn0), new Integer(100));
        assertEquals(bpti.find(9, txn0), new Integer(101));
        assertEquals(bpti.find(13, txn0), new Integer(102));
        assertEquals(bpti.find(20, txn0), new Integer(103));

        bpti.insert(16, 104, txn0);
        assertEquals(bpti.find(16, txn0), new Integer(104));

        bpti.insert(50, 105, txn0);
        assertEquals(bpti.find(50, txn0), new Integer(105));

        bpti.insert(1, 106, txn0);
        assertEquals(bpti.find(1, txn0), new Integer(106));

        bpti.insert(80, 107, txn0);
        assertEquals(bpti.find(80, txn0), new Integer(107));

        bpti.insert(8, 108, txn0);
        assertEquals(bpti.find(8, txn0), new Integer(108));

        bpti.insert(12, 109, txn0);
        assertEquals(bpti.find(12, txn0), new Integer(109));

        bpti.insert(11, 110, txn0);
        assertEquals(bpti.find(11, txn0), new Integer(110));

        bpti.insert(17, 111, txn0);
        assertEquals(bpti.find(17, txn0), new Integer(111));

        bpti.insert(18, 112, txn0);
        assertEquals(bpti.find(18, txn0), new Integer(112));

        bpti.insert(14, 113, txn0);
        assertEquals(bpti.find(14, txn0), new Integer(113));

        bpti.insert(15, 114, txn0);
        assertEquals(bpti.find(15, txn0), new Integer(114));

        bpti.insert(120, 115, txn0);
        assertEquals(bpti.find(120, txn0), new Integer(115));

        bpti.insert(100, 116, txn0);
        assertEquals(bpti.find(100, txn0), new Integer(116));

        bpti.traverseAllNodes();

        bufferManager.flushAllPages();

        diskManager.close();
        diskManager.dropFile(dbName);
    }

    @Test
    public void insertScaleTest() throws IOException, ClassNotFoundException, InterruptedException {
        int bufferSize = 1000;
        BufferManager bufferManager = new BufferManager(bufferSize, diskManager, logManager);

        BPlusTreeIndex<Integer, Integer> bpti = new BPlusTreeIndex<>(bufferManager, Config.INVALID_PAGE_ID, 100, 100);
        Transaction txn0 = transactionManager.begin();

        int max = 1000, i;
        for(i = 0; i < max; i++) {
            bpti.insert(i, i, txn0);
            assertEquals(bpti.find(i, txn0), new Integer(i));
        }

        for(i = 0; i < max; i++) {
//            bpti.insert(i, i);
            assertEquals(bpti.find(i, txn0), new Integer(i));
        }
    }

    @Test
    public void insertPersistScaleTest() throws IOException, ClassNotFoundException, InterruptedException {
        int bufferSize = 100000;
        BufferManager bufferManager = new BufferManager(bufferSize, diskManager, logManager);

        BPlusTreeIndex<Integer, Integer> bpti = new BPlusTreeIndex<>(bufferManager, Config.INVALID_PAGE_ID, 100, 100);
        Transaction txn0 = transactionManager.begin();

        int max = 100000, i;
        for(i = 0; i < max; i++) {
            bpti.insert(i, i, txn0);
        }

        bufferManager.flushAllPages();

        for(i = 0; i < max; i++) {
            assertEquals(bpti.find(i, txn0), new Integer(i));
        }

        diskManager.close();
    }

    @Test
    public void traverseLeafNodesTest() throws IOException, ClassNotFoundException, InterruptedException {
        int bufferSize = 100;
        BufferManager bufferManager = new BufferManager(bufferSize, diskManager, logManager);

        BPlusTreeIndex<Integer, Integer> bpti = new BPlusTreeIndex<>(bufferManager, Config.INVALID_PAGE_ID, 3, 3);
        Transaction txn0 = transactionManager.begin();

        bpti.insert(5, 100, txn0);
        bpti.insert(9, 101, txn0);
        bpti.insert(13, 102, txn0);
        bpti.insert(20, 103, txn0);
        assertEquals(bpti.find(5, txn0), new Integer(100));
        assertEquals(bpti.find(9, txn0), new Integer(101));
        assertEquals(bpti.find(13, txn0), new Integer(102));
        assertEquals(bpti.find(20, txn0), new Integer(103));

        bpti.insert(16, 104, txn0);
        assertEquals(bpti.find(16, txn0), new Integer(104));

        bpti.insert(50, 105, txn0);
        assertEquals(bpti.find(50, txn0), new Integer(105));

        bpti.insert(1, 106, txn0);
        assertEquals(bpti.find(1, txn0), new Integer(106));

        bpti.insert(80, 107, txn0);
        assertEquals(bpti.find(80, txn0), new Integer(107));

        bpti.insert(8, 108, txn0);
        assertEquals(bpti.find(8, txn0), new Integer(108));

        bpti.insert(12, 109, txn0);
        assertEquals(bpti.find(12, txn0), new Integer(109));

        bpti.insert(11, 110, txn0);
        assertEquals(bpti.find(11, txn0), new Integer(110));

        bpti.insert(17, 111, txn0);
        assertEquals(bpti.find(17, txn0), new Integer(111));

        bpti.insert(18, 112, txn0);
        assertEquals(bpti.find(18, txn0), new Integer(112));

        bpti.insert(14, 113, txn0);
        assertEquals(bpti.find(14, txn0), new Integer(113));

        bpti.insert(15, 114, txn0);
        assertEquals(bpti.find(15, txn0), new Integer(114));

        bpti.insert(120, 115, txn0);
        assertEquals(bpti.find(120, txn0), new Integer(115));

        bpti.insert(100, 116, txn0);
        assertEquals(bpti.find(100, txn0), new Integer(116));

//        bpti.traverseLeafNodes(120);

        diskManager.close();
    }

    @Test
    public void deleteWithRedistributeAndMergeTest() throws IOException, ClassNotFoundException, InterruptedException {
        int bufferSize = 100;
        BufferManager bufferManager = new BufferManager(bufferSize, diskManager, logManager);

        BPlusTreeIndex<Integer, Integer> bpti = new BPlusTreeIndex<>(bufferManager, Config.INVALID_PAGE_ID, 3, 3);
        Transaction txn0 = transactionManager.begin();

        bpti.insert(5, 100, txn0);
        bpti.insert(9, 101, txn0);
        bpti.insert(13, 102, txn0);
        bpti.insert(20, 103, txn0);
        bpti.insert(30, 104, txn0);
        assertEquals(bpti.find(5, txn0), new Integer(100));
        assertEquals(bpti.find(9, txn0), new Integer(101));
        assertEquals(bpti.find(13, txn0), new Integer(102));
        assertEquals(bpti.find(20, txn0), new Integer(103));
        assertEquals(bpti.find(30, txn0), new Integer(104));

//        bpti.traverseLeafNodes();

        bpti.delete(13, txn0);
        assertNull(bpti.find(13, txn0));
//        bpti.traverseAllNodes();

        bpti.delete(20, txn0);
        assertNull(bpti.find(20, txn0));
//        bpti.traverseAllNodes();

        assertEquals(bpti.find(5, txn0), new Integer(100));
        assertEquals(bpti.find(9, txn0), new Integer(101));
        assertEquals(bpti.find(30, txn0), new Integer(104));

        bpti.traverseLeafNodes();

        diskManager.close();
    }

    @Test
    public void deleteScaleTest() throws IOException, ClassNotFoundException, InterruptedException {
        int bufferSize = 10000;
        BufferManager bufferManager = new BufferManager(bufferSize, diskManager, logManager);

        BPlusTreeIndex<Integer, Integer> bpti = new BPlusTreeIndex<>(bufferManager, Config.INVALID_PAGE_ID, 200, 200);
        Transaction txn0 = transactionManager.begin();

        int max = 100000, i;
        for(i = 0; i < max; i++) {
            bpti.insert(i, i, txn0);
//            assertEquals(bpti.find(i), new Integer(i));
        }

        for(i = 0; i < max; i++) {
            bpti.delete(i, txn0);
            assertNull(bpti.find(i, txn0));
            if (i < max - 1)
                assertEquals(bpti.find(i + 1, txn0), new Integer(i + 1));
        }

        diskManager.close();
    }

    @Test
    public void scanLeafNodeTest() throws InterruptedException {
        // buffer size is at least 5 here, meaning at least 20 kb memory assigned
        int bufferSize = 5;
        BufferManager bufferManager = new BufferManager(bufferSize, diskManager, logManager);

        BPlusTreeIndex<Integer, Integer> bpti = new BPlusTreeIndex<>(bufferManager, Config.INVALID_PAGE_ID, 144, 144);
        Transaction txn0 = transactionManager.begin();

        int max = 100000, i;
        for(i = 0; i < max; i++) {
            bpti.insert(i, i, txn0);
        }

        ArrayList<Integer> res = new ArrayList<>(bpti.scanLeafNode(0, true, txn0));
        i = 0;
        for (Integer value : res) {
            assertEquals(value, new Integer(i));
            i++;
        }

        res = new ArrayList<>(bpti.scanLeafNode(max, false, txn0));
        Collections.reverse(res);
        i = 0;
        for (Integer value : res) {
            assertEquals(value, new Integer(i));
            i++;
        }

        res = new ArrayList<>(bpti.scanLeafNode(50000, true, txn0));
        i = 50000;
        for (Integer value : res) {
            assertEquals(value, new Integer(i));
            i++;
        }

        diskManager.close();
    }

    @Test
    public void multipleThreadOperationTest() throws InterruptedException {
        class T0Operation implements Runnable {
            private BPlusTreeIndex<Integer, Integer> bpti;
            private Transaction txn;
            public T0Operation(BPlusTreeIndex<Integer, Integer> bpti, Transaction txn) {
                this.bpti = bpti;
                this.txn = txn;
            }

            @Override
            public void run() {
                try {
                    TimeUnit.MILLISECONDS.sleep(Math.round(200));
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                System.out.println("Finding 100 in txn " + txn.getTxnId() + ": result is " + bpti.find(100, txn));
            }
        }

        class T1Operation implements Runnable {
            private BPlusTreeIndex<Integer, Integer> bpti;
            private Transaction txn;
            public T1Operation(BPlusTreeIndex<Integer, Integer> bpti, Transaction txn) {
                this.bpti = bpti;
                this.txn = txn;
            }

            @Override
            public void run() {
                try {
                    TimeUnit.MILLISECONDS.sleep(Math.round(200));
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                bpti.insert(100, 116, txn);
            }
        }

        class T2Operation implements Runnable {
            private BPlusTreeIndex<Integer, Integer> bpti;
            private Transaction txn;
            public T2Operation(BPlusTreeIndex<Integer, Integer> bpti, Transaction txn) {
                this.bpti = bpti;
                this.txn = txn;
            }

            @Override
            public void run() {
                try {
                    TimeUnit.MILLISECONDS.sleep(Math.round(200));
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                System.out.println("Finding 1 in txn " + txn.getTxnId() + ": result is " + bpti.find(1, txn));
            }
        }

        int bufferSize = 10;
        BufferManager bufferManager = new BufferManager(bufferSize, diskManager, logManager);
        BPlusTreeIndex<Integer, Integer> bpti = new BPlusTreeIndex<>(bufferManager, Config.INVALID_PAGE_ID, 3, 3);
        Transaction txn0 = transactionManager.begin();
        Transaction txn1 = transactionManager.begin();
        Transaction txn2 = transactionManager.begin();
        Transaction txn3 = transactionManager.begin();
        Transaction txn4 = transactionManager.begin();
        Transaction txn5 = transactionManager.begin();

        bpti.insert(5, 100, txn0);     // safe
        bpti.insert(9, 101, txn0);     // safe
        bpti.insert(13, 102, txn0);    // split in leaf
        bpti.insert(20, 103, txn0);    // split in leaf
        bpti.insert(16, 104, txn0);    // split in leaf and inner
        bpti.insert(50, 105, txn0);    // split in leaf
        bpti.insert(1, 106, txn0);     // safe
        bpti.insert(80, 107, txn0);    // split in leaf and inner
        bpti.insert(8, 108, txn0);     // split in leaf
        bpti.insert(12, 109, txn0);    // safe
        bpti.insert(11, 110, txn0);    // split in leaf and inner
        bpti.insert(17, 111, txn0);    // safe
        bpti.insert(18, 112, txn0);    // split in leaf
        bpti.insert(14, 113, txn0);    // safe
        bpti.insert(15, 114, txn0);    // split in leaf and inner
        bpti.insert(120, 115, txn0);   // split in leaf
//        bpt.insert(100, 116, txn0);   // split in leaf and inner

//        bpt.find(100, txn0);
//
//        bpt.traverseLeafNodes();

        ExecutorService executorService = Executors.newCachedThreadPool();
        executorService.submit(new T0Operation(bpti, txn1));
        executorService.submit(new T1Operation(bpti, txn2));
        executorService.submit(new T0Operation(bpti, txn3));
        executorService.submit(new T2Operation(bpti, txn4));

        // give detector enough time to finish detecting
        TimeUnit.SECONDS.sleep(1);
        executorService.shutdown();

        bufferManager.flushAllPages();

        assertEquals(bpti.find(100, txn5), new Integer(116));

        diskManager.close();
    }
}
