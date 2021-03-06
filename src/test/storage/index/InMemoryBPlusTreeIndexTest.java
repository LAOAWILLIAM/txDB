package test.storage.index;

import org.junit.Test;

import java.sql.Time;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.*;

import txDB.concurrency.Transaction;
import txDB.storage.index.InMemoryBPlusTreeIndex;
import txDB.storage.table.RecordID;

public class InMemoryBPlusTreeIndexTest {
    @Test
    public void subListTest() {
        List<Integer> l1 = new ArrayList<>();

        l1.add(1);
        l1.add(2);
        l1.add(3);
        l1.add(4);
        l1.add(5);
        l1.add(6);

        assertEquals(l1, new ArrayList<>(Arrays.asList(1, 2, 3, 4, 5, 6)));

        List<Integer> l2 = new ArrayList<>(l1.subList(4, 6));

        assertEquals(l2, new ArrayList<>(Arrays.asList(5, 6)));

        l1.subList(4, 6).clear();

        assertEquals(l1, new ArrayList<>(Arrays.asList(1, 2, 3, 4)));
        assertEquals(l2, new ArrayList<>(Arrays.asList(5, 6)));
    }

    @Test
    public void rootAsLeafNodeTest() {
        InMemoryBPlusTreeIndex<Integer, Integer> bpt = new InMemoryBPlusTreeIndex<>(3);
        InMemoryBPlusTreeIndex<Integer, Integer>.Transaction txn0 = bpt.getTransactionManager().begin();
        assertNull(bpt.find(5, txn0));

        bpt.insert(5, 100, txn0);
        bpt.insert(9, 101, txn0);
        assertEquals(bpt.find(5, txn0), new Integer(100));
        assertEquals(bpt.find(9, txn0), new Integer(101));
    }

    @Test
    public void rootAsLeafNodeFirstSplitTest() {
        InMemoryBPlusTreeIndex<Integer, Integer> bpt = new InMemoryBPlusTreeIndex<>(3);
        InMemoryBPlusTreeIndex<Integer, Integer>.Transaction txn0 = bpt.getTransactionManager().begin();
        assertNull(bpt.find(5, txn0));

        bpt.insert(5, 100, txn0);
        bpt.insert(9, 101, txn0);
        bpt.insert(13, 102, txn0);
        assertEquals(bpt.find(5, txn0), new Integer(100));
        assertEquals(bpt.find(9, txn0), new Integer(101));
        assertEquals(bpt.find(13, txn0), new Integer(102));
    }

    @Test
    public void rootAsLeafNodeFirstSplitAndInsertTest() {
        InMemoryBPlusTreeIndex<Integer, Integer> bpt = new InMemoryBPlusTreeIndex<>(3);
        InMemoryBPlusTreeIndex<Integer, Integer>.Transaction txn0 = bpt.getTransactionManager().begin();
        assertNull(bpt.find(5, txn0));

        bpt.insert(5, 100, txn0);
        bpt.insert(9, 101, txn0);
        bpt.insert(13, 102, txn0);
        bpt.insert(20, 103, txn0);
        assertEquals(bpt.find(5, txn0), new Integer(100));
        assertEquals(bpt.find(9, txn0), new Integer(101));
        assertEquals(bpt.find(13, txn0), new Integer(102));
        assertEquals(bpt.find(20, txn0), new Integer(103));

        bpt.insert(1, 104, txn0);
        assertEquals(bpt.find(1, txn0), new Integer(104));

        bpt.insert(10, 105, txn0);
        assertEquals(bpt.find(10, txn0), new Integer(105));
    }

    @Test
    public void internalNodeSplitTest() {
        InMemoryBPlusTreeIndex<Integer, Integer> bpt = new InMemoryBPlusTreeIndex<>(3);
        InMemoryBPlusTreeIndex<Integer, Integer>.Transaction txn0 = bpt.getTransactionManager().begin();
        assertNull(bpt.find(5, txn0));

        bpt.insert(5, 100, txn0);
        bpt.insert(9, 101, txn0);
        bpt.insert(13, 102, txn0);
        bpt.insert(20, 103, txn0);
        assertEquals(bpt.find(5, txn0), new Integer(100));
        assertEquals(bpt.find(9, txn0), new Integer(101));
        assertEquals(bpt.find(13, txn0), new Integer(102));
        assertEquals(bpt.find(20, txn0), new Integer(103));

        bpt.insert(16, 104, txn0);
        assertEquals(bpt.find(16, txn0), new Integer(104));

        bpt.insert(50, 105, txn0);
        assertEquals(bpt.find(50, txn0), new Integer(105));

        bpt.insert(1, 106, txn0);
        assertEquals(bpt.find(1, txn0), new Integer(106));

        bpt.insert(80, 107, txn0);
        assertEquals(bpt.find(80, txn0), new Integer(107));

        bpt.insert(8, 108, txn0);
        assertEquals(bpt.find(8, txn0), new Integer(108));

        bpt.insert(12, 109, txn0);
        assertEquals(bpt.find(12, txn0), new Integer(109));

        bpt.insert(11, 110, txn0);
        assertEquals(bpt.find(11, txn0), new Integer(110));

        bpt.insert(17, 111, txn0);
        assertEquals(bpt.find(17, txn0), new Integer(111));

        bpt.insert(18, 112, txn0);
        assertEquals(bpt.find(18, txn0), new Integer(112));

        bpt.insert(14, 113, txn0);
        assertEquals(bpt.find(14, txn0), new Integer(113));

        bpt.insert(15, 114, txn0);
        assertEquals(bpt.find(15, txn0), new Integer(114));

        bpt.insert(120, 115, txn0);
        assertEquals(bpt.find(120, txn0), new Integer(115));

        bpt.insert(100, 116, txn0);
        assertEquals(bpt.find(100, txn0), new Integer(116));

        bpt.traverseAllNodes();
    }

    @Test
    public void insertScaleTest() {
        InMemoryBPlusTreeIndex<Integer, Integer> bpt = new InMemoryBPlusTreeIndex<>(10);
        InMemoryBPlusTreeIndex<Integer, Integer>.Transaction txn0 = bpt.getTransactionManager().begin();
        assertNull(bpt.find(5, txn0));

        int max = 10000000, i;
        for(i = 0; i < max; i++) {
            bpt.insert(i, i, txn0);
//            assertEquals(bpt.find(i, txn0), new Integer(i));
        }

        for(i = 0; i < max; i++) {
            assertEquals(bpt.find(i, txn0), new Integer(i));
        }

//        bpt.traverseAllNodes();
    }

    @Test
    public void traverseLeafNodesTest() {
        InMemoryBPlusTreeIndex<Integer, Integer> bpt = new InMemoryBPlusTreeIndex<>(3);
        InMemoryBPlusTreeIndex<Integer, Integer>.Transaction txn0 = bpt.getTransactionManager().begin();
        assertNull(bpt.find(5, txn0));

        bpt.insert(5, 100, txn0);
        bpt.insert(9, 101, txn0);
        bpt.insert(13, 102, txn0);
        bpt.insert(20, 103, txn0);
        assertEquals(bpt.find(5, txn0), new Integer(100));
        assertEquals(bpt.find(9, txn0), new Integer(101));
        assertEquals(bpt.find(13, txn0), new Integer(102));
        assertEquals(bpt.find(20, txn0), new Integer(103));

        bpt.insert(16, 104, txn0);
        assertEquals(bpt.find(16, txn0), new Integer(104));

        bpt.insert(50, 105, txn0);
        assertEquals(bpt.find(50, txn0), new Integer(105));

        bpt.insert(1, 106, txn0);
        assertEquals(bpt.find(1, txn0), new Integer(106));

        bpt.insert(80, 107, txn0);
        assertEquals(bpt.find(80, txn0), new Integer(107));

        bpt.insert(8, 108, txn0);
        assertEquals(bpt.find(8, txn0), new Integer(108));

        bpt.insert(12, 109, txn0);
        assertEquals(bpt.find(12, txn0), new Integer(109));

        bpt.insert(11, 110, txn0);
        assertEquals(bpt.find(11, txn0), new Integer(110));

        bpt.insert(17, 111, txn0);
        assertEquals(bpt.find(17, txn0), new Integer(111));

        bpt.insert(18, 112, txn0);
        assertEquals(bpt.find(18, txn0), new Integer(112));

        bpt.insert(14, 113, txn0);
        assertEquals(bpt.find(14, txn0), new Integer(113));

        bpt.insert(15, 114, txn0);
        assertEquals(bpt.find(15, txn0), new Integer(114));

        bpt.insert(120, 115, txn0);
        assertEquals(bpt.find(120, txn0), new Integer(115));

        bpt.insert(100, 116, txn0);
        assertEquals(bpt.find(100, txn0), new Integer(116));

        bpt.traverseLeafNodes();
    }

    @Test
    public void deleteWithoutRedistributeAndMergeTest() {
        InMemoryBPlusTreeIndex<Integer, Integer> bpt = new InMemoryBPlusTreeIndex<>(5);
        InMemoryBPlusTreeIndex<Integer, Integer>.Transaction txn0 = bpt.getTransactionManager().begin();
        assertNull(bpt.find(5, txn0));

        bpt.insert(5, 100, txn0);
        bpt.insert(9, 101, txn0);
        bpt.insert(13, 102, txn0);
        bpt.insert(20, 103, txn0);
        bpt.insert(30, 104, txn0);
        assertEquals(bpt.find(5, txn0), new Integer(100));
        assertEquals(bpt.find(9, txn0), new Integer(101));
        assertEquals(bpt.find(13, txn0), new Integer(102));
        assertEquals(bpt.find(20, txn0), new Integer(103));
        assertEquals(bpt.find(30, txn0), new Integer(104));

        bpt.delete(13, txn0);
        assertNull(bpt.find(13, txn0));

        assertEquals(bpt.find(20, txn0), new Integer(103));
    }

    @Test
    public void deleteWithRedistributeAndMergeTest() {
        InMemoryBPlusTreeIndex<Integer, Integer> bpt = new InMemoryBPlusTreeIndex<>(3);
        InMemoryBPlusTreeIndex<Integer, Integer>.Transaction txn0 = bpt.getTransactionManager().begin();
        assertNull(bpt.find(5, txn0));

        bpt.insert(5, 100, txn0);
        bpt.insert(9, 101, txn0);
        bpt.insert(13, 102, txn0);
        bpt.insert(20, 103, txn0);
        bpt.insert(30, 104, txn0);
        assertEquals(bpt.find(5, txn0), new Integer(100));
        assertEquals(bpt.find(9, txn0), new Integer(101));
        assertEquals(bpt.find(13, txn0), new Integer(102));
        assertEquals(bpt.find(20, txn0), new Integer(103));
        assertEquals(bpt.find(30, txn0), new Integer(104));

//        bpt.traverseAllNodes();

        bpt.delete(13, txn0);
        assertNull(bpt.find(13, txn0));
        bpt.traverseAllNodes();

        bpt.delete(20, txn0);
        assertNull(bpt.find(20, txn0));

        assertEquals(bpt.find(5, txn0), new Integer(100));
        assertEquals(bpt.find(9, txn0), new Integer(101));
        assertEquals(bpt.find(30, txn0), new Integer(104));

//        bpt.traverseLeafNodes();
    }

    @Test
    public void deleteScaleTest() {
        InMemoryBPlusTreeIndex<Integer, Integer> bpt = new InMemoryBPlusTreeIndex<>(300);
        InMemoryBPlusTreeIndex<Integer, Integer>.Transaction txn0 = bpt.getTransactionManager().begin();
        assertNull(bpt.find(5, txn0));

        int max = 1000000, i;
        for(i = 0; i < max; i++) {
            bpt.insert(i, i, txn0);
//            assertEquals(bpt.find(i, txn0), new Integer(i));
        }

        for(i = 0; i < max; i++) {
            bpt.delete(i, txn0);
            assertNull(bpt.find(i, txn0));
//            bpt.traverseAllNodes();
//            System.out.println();
//            if (i < max - 1)
//                assertEquals(bpt.find(i + 1), new Integer(i + 1));
        }
    }

    @Test
    public void scanLeafNodeTest() {
        InMemoryBPlusTreeIndex<Integer, Integer> bpt = new InMemoryBPlusTreeIndex<>(300);
        InMemoryBPlusTreeIndex<Integer, Integer>.Transaction txn0 = bpt.getTransactionManager().begin();

        int max = 10000000, i;
        for(i = 0; i < max; i++) {
            bpt.insert(i, i, txn0);
        }

        ArrayList<Integer> res = new ArrayList<>(bpt.scanLeafNode(0, true, txn0));
        i = 0;
        for (Integer value : res) {
            assertEquals(value, new Integer(i));
            i++;
        }

        res = new ArrayList<>(bpt.scanLeafNode(max, false, txn0));
        Collections.reverse(res);
        i = 0;
        for (Integer value : res) {
            assertEquals(value, new Integer(i));
            i++;
        }

        res = new ArrayList<>(bpt.scanLeafNode(5000000, true, txn0));
        i = 5000000;
        for (Integer value : res) {
            assertEquals(value, new Integer(i));
            i++;
        }
    }

    @Test
    public void multipleThreadOperationTest() throws InterruptedException {
        class T0Operation implements Runnable {
            private InMemoryBPlusTreeIndex<Integer, Integer> bpt;
            private InMemoryBPlusTreeIndex<Integer, Integer>.Transaction txn;
            public T0Operation(InMemoryBPlusTreeIndex<Integer, Integer> bpt, InMemoryBPlusTreeIndex<Integer, Integer>.Transaction txn) {
                this.bpt = bpt;
                this.txn = txn;
            }

            @Override
            public void run() {
                try {
                    TimeUnit.MILLISECONDS.sleep(Math.round(200));
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                System.out.println("Finding 100 in txn " + txn.getTxnId() + ": result is " + bpt.find(100, txn));
            }
        }

        class T1Operation implements Runnable {
            private InMemoryBPlusTreeIndex<Integer, Integer> bpt;
            private InMemoryBPlusTreeIndex<Integer, Integer>.Transaction txn;
            public T1Operation(InMemoryBPlusTreeIndex<Integer, Integer> bpt, InMemoryBPlusTreeIndex<Integer, Integer>.Transaction txn) {
                this.bpt = bpt;
                this.txn = txn;
            }

            @Override
            public void run() {
                try {
                    TimeUnit.MILLISECONDS.sleep(Math.round(200));
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                bpt.insert(100, 116, txn);
            }
        }

        class T2Operation implements Runnable {
            private InMemoryBPlusTreeIndex<Integer, Integer> bpt;
            private InMemoryBPlusTreeIndex<Integer, Integer>.Transaction txn;
            public T2Operation(InMemoryBPlusTreeIndex<Integer, Integer> bpt, InMemoryBPlusTreeIndex<Integer, Integer>.Transaction txn) {
                this.bpt = bpt;
                this.txn = txn;
            }

            @Override
            public void run() {
                try {
                    TimeUnit.MILLISECONDS.sleep(Math.round(200));
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                System.out.println("Finding 1 in txn " + txn.getTxnId() + ": result is " + bpt.find(1, txn));
            }
        }

        InMemoryBPlusTreeIndex<Integer, Integer> bpt = new InMemoryBPlusTreeIndex<>(3);
        InMemoryBPlusTreeIndex<Integer, Integer>.Transaction txn0 = bpt.getTransactionManager().begin();
        InMemoryBPlusTreeIndex<Integer, Integer>.Transaction txn1 = bpt.getTransactionManager().begin();
        InMemoryBPlusTreeIndex<Integer, Integer>.Transaction txn2 = bpt.getTransactionManager().begin();
        InMemoryBPlusTreeIndex<Integer, Integer>.Transaction txn3 = bpt.getTransactionManager().begin();
        InMemoryBPlusTreeIndex<Integer, Integer>.Transaction txn4 = bpt.getTransactionManager().begin();
        InMemoryBPlusTreeIndex<Integer, Integer>.Transaction txn5 = bpt.getTransactionManager().begin();

        bpt.insert(5, 100, txn0);     // safe
        bpt.insert(9, 101, txn0);     // safe
        bpt.insert(13, 102, txn0);    // split in leaf
        bpt.insert(20, 103, txn0);    // split in leaf
        bpt.insert(16, 104, txn0);    // split in leaf and inner
        bpt.insert(50, 105, txn0);    // split in leaf
        bpt.insert(1, 106, txn0);     // safe
        bpt.insert(80, 107, txn0);    // split in leaf and inner
        bpt.insert(8, 108, txn0);     // split in leaf
        bpt.insert(12, 109, txn0);    // safe
        bpt.insert(11, 110, txn0);    // split in leaf and inner
        bpt.insert(17, 111, txn0);    // safe
        bpt.insert(18, 112, txn0);    // split in leaf
        bpt.insert(14, 113, txn0);    // safe
        bpt.insert(15, 114, txn0);    // split in leaf and inner
        bpt.insert(120, 115, txn0);   // split in leaf
//        bpt.insert(100, 116, txn0);   // split in leaf and inner

//        bpt.find(100, txn0);
//
//        bpt.traverseLeafNodes();

        ExecutorService executorService = Executors.newCachedThreadPool();
        executorService.submit(new T0Operation(bpt, txn1));
        executorService.submit(new T1Operation(bpt, txn2));
        executorService.submit(new T0Operation(bpt, txn3));
        executorService.submit(new T2Operation(bpt, txn4));

        // give detector enough time to finish detecting
        TimeUnit.SECONDS.sleep(1);
        executorService.shutdown();

        assertEquals(bpt.find(100, txn5), new Integer(116));
    }
}
