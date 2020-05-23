package test.storage.index;

import org.junit.Test;

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
        assertNull(bpt.find(5, null));

        bpt.insert(5, 100, null);
        bpt.insert(9, 101, null);
        assertEquals(bpt.find(5, null), new Integer(100));
        assertEquals(bpt.find(9, null), new Integer(101));
    }

    @Test
    public void rootAsLeafNodeFirstSplitTest() {
        InMemoryBPlusTreeIndex<Integer, Integer> bpt = new InMemoryBPlusTreeIndex<>(3);
        assertNull(bpt.find(5, null));

        bpt.insert(5, 100, null);
        bpt.insert(9, 101, null);
        bpt.insert(13, 102, null);
        assertEquals(bpt.find(5, null), new Integer(100));
        assertEquals(bpt.find(9, null), new Integer(101));
        assertEquals(bpt.find(13, null), new Integer(102));
    }

    @Test
    public void rootAsLeafNodeFirstSplitAndInsertTest() {
        InMemoryBPlusTreeIndex<Integer, Integer> bpt = new InMemoryBPlusTreeIndex<>(3);
        assertNull(bpt.find(5, null));

        bpt.insert(5, 100, null);
        bpt.insert(9, 101, null);
        bpt.insert(13, 102, null);
        bpt.insert(20, 103, null);
        assertEquals(bpt.find(5, null), new Integer(100));
        assertEquals(bpt.find(9, null), new Integer(101));
        assertEquals(bpt.find(13, null), new Integer(102));
        assertEquals(bpt.find(20, null), new Integer(103));

        bpt.insert(1, 104, null);
        assertEquals(bpt.find(1, null), new Integer(104));

        bpt.insert(10, 105, null);
        assertEquals(bpt.find(10, null), new Integer(105));
    }

    @Test
    public void internalNodeSplitTest() {
        InMemoryBPlusTreeIndex<Integer, Integer> bpt = new InMemoryBPlusTreeIndex<>(3);
        assertNull(bpt.find(5, null));

        bpt.insert(5, 100, null);
        bpt.insert(9, 101, null);
        bpt.insert(13, 102, null);
        bpt.insert(20, 103, null);
        assertEquals(bpt.find(5, null), new Integer(100));
        assertEquals(bpt.find(9, null), new Integer(101));
        assertEquals(bpt.find(13, null), new Integer(102));
        assertEquals(bpt.find(20, null), new Integer(103));

        bpt.insert(16, 104, null);
        assertEquals(bpt.find(16, null), new Integer(104));

        bpt.insert(50, 105, null);
        assertEquals(bpt.find(50, null), new Integer(105));

        bpt.insert(1, 106, null);
        assertEquals(bpt.find(1, null), new Integer(106));

        bpt.insert(80, 107, null);
        assertEquals(bpt.find(80, null), new Integer(107));

        bpt.insert(8, 108, null);
        assertEquals(bpt.find(8, null), new Integer(108));

        bpt.insert(12, 109, null);
        assertEquals(bpt.find(12, null), new Integer(109));

        bpt.insert(11, 110, null);
        assertEquals(bpt.find(11, null), new Integer(110));

        bpt.insert(17, 111, null);
        assertEquals(bpt.find(17, null), new Integer(111));

        bpt.insert(18, 112, null);
        assertEquals(bpt.find(18, null), new Integer(112));

        bpt.insert(14, 113, null);
        assertEquals(bpt.find(14, null), new Integer(113));

        bpt.insert(15, 114, null);
        assertEquals(bpt.find(15, null), new Integer(114));

        bpt.insert(120, 115, null);
        assertEquals(bpt.find(120, null), new Integer(115));

        bpt.insert(100, 116, null);
        assertEquals(bpt.find(100, null), new Integer(116));

        bpt.traverseAllNodes();
    }

    @Test
    public void insertScaleTest() {
        InMemoryBPlusTreeIndex<Integer, Integer> bpt = new InMemoryBPlusTreeIndex<>(10);
        assertNull(bpt.find(5, null));

        int max = 10000000, i;
        for(i = 0; i < max; i++) {
            bpt.insert(i, i, null);
//            assertEquals(bpt.find(i, null), new Integer(i));
        }

        for(i = 0; i < max; i++) {
            assertEquals(bpt.find(i, null), new Integer(i));
        }

//        bpt.traverseAllNodes();
    }

    @Test
    public void traverseLeafNodesTest() {
        InMemoryBPlusTreeIndex<Integer, Integer> bpt = new InMemoryBPlusTreeIndex<>(3);
        assertNull(bpt.find(5, null));

        bpt.insert(5, 100, null);
        bpt.insert(9, 101, null);
        bpt.insert(13, 102, null);
        bpt.insert(20, 103, null);
        assertEquals(bpt.find(5, null), new Integer(100));
        assertEquals(bpt.find(9, null), new Integer(101));
        assertEquals(bpt.find(13, null), new Integer(102));
        assertEquals(bpt.find(20, null), new Integer(103));

        bpt.insert(16, 104, null);
        assertEquals(bpt.find(16, null), new Integer(104));

        bpt.insert(50, 105, null);
        assertEquals(bpt.find(50, null), new Integer(105));

        bpt.insert(1, 106, null);
        assertEquals(bpt.find(1, null), new Integer(106));

        bpt.insert(80, 107, null);
        assertEquals(bpt.find(80, null), new Integer(107));

        bpt.insert(8, 108, null);
        assertEquals(bpt.find(8, null), new Integer(108));

        bpt.insert(12, 109, null);
        assertEquals(bpt.find(12, null), new Integer(109));

        bpt.insert(11, 110, null);
        assertEquals(bpt.find(11, null), new Integer(110));

        bpt.insert(17, 111, null);
        assertEquals(bpt.find(17, null), new Integer(111));

        bpt.insert(18, 112, null);
        assertEquals(bpt.find(18, null), new Integer(112));

        bpt.insert(14, 113, null);
        assertEquals(bpt.find(14, null), new Integer(113));

        bpt.insert(15, 114, null);
        assertEquals(bpt.find(15, null), new Integer(114));

        bpt.insert(120, 115, null);
        assertEquals(bpt.find(120, null), new Integer(115));

        bpt.insert(100, 116, null);
        assertEquals(bpt.find(100, null), new Integer(116));

        bpt.traverseLeafNodes();
    }

    @Test
    public void deleteWithoutRedistributeAndMergeTest() {
        InMemoryBPlusTreeIndex<Integer, Integer> bpt = new InMemoryBPlusTreeIndex<>(5);
        assertNull(bpt.find(5, null));

        bpt.insert(5, 100, null);
        bpt.insert(9, 101, null);
        bpt.insert(13, 102, null);
        bpt.insert(20, 103, null);
        bpt.insert(30, 104, null);
        assertEquals(bpt.find(5, null), new Integer(100));
        assertEquals(bpt.find(9, null), new Integer(101));
        assertEquals(bpt.find(13, null), new Integer(102));
        assertEquals(bpt.find(20, null), new Integer(103));
        assertEquals(bpt.find(30, null), new Integer(104));

        bpt.delete(13);
        assertNull(bpt.find(13, null));

        assertEquals(bpt.find(20, null), new Integer(103));
    }

    @Test
    public void deleteWithRedistributeAndMergeTest() {
        InMemoryBPlusTreeIndex<Integer, Integer> bpt = new InMemoryBPlusTreeIndex<>(3);
        assertNull(bpt.find(5, null));

        bpt.insert(5, 100, null);
        bpt.insert(9, 101, null);
        bpt.insert(13, 102, null);
        bpt.insert(20, 103, null);
        bpt.insert(30, 104, null);
        assertEquals(bpt.find(5, null), new Integer(100));
        assertEquals(bpt.find(9, null), new Integer(101));
        assertEquals(bpt.find(13, null), new Integer(102));
        assertEquals(bpt.find(20, null), new Integer(103));
        assertEquals(bpt.find(30, null), new Integer(104));

//        bpt.traverseAllNodes();

        bpt.delete(13);
        assertNull(bpt.find(13, null));
        bpt.traverseAllNodes();

        bpt.delete(20);
        assertNull(bpt.find(20, null));

        assertEquals(bpt.find(5, null), new Integer(100));
        assertEquals(bpt.find(9, null), new Integer(101));
        assertEquals(bpt.find(30, null), new Integer(104));

//        bpt.traverseLeafNodes();
    }

    @Test
    public void deleteScaleTest() {
        InMemoryBPlusTreeIndex<Integer, Integer> bpt = new InMemoryBPlusTreeIndex<>(300);
        assertNull(bpt.find(5, null));

        int max = 1000000, i;
        for(i = 0; i < max; i++) {
            bpt.insert(i, i, null);
//            assertEquals(bpt.find(i, null), new Integer(i));
        }

        for(i = 0; i < max; i++) {
            bpt.delete(i);
            assertNull(bpt.find(i, null));
//            bpt.traverseAllNodes();
//            System.out.println();
//            if (i < max - 1)
//                assertEquals(bpt.find(i + 1), new Integer(i + 1));
        }
    }

    @Test
    public void scanLeafNodeTest() {
        InMemoryBPlusTreeIndex<Integer, Integer> bpt = new InMemoryBPlusTreeIndex<>(300);

        int max = 10000000, i;
        for(i = 0; i < max; i++) {
            bpt.insert(i, i, null);
        }

        ArrayList<Integer> res = new ArrayList<>(bpt.scanLeafNode(0, true, null));
        i = 0;
        for (Integer value : res) {
            assertEquals(value, new Integer(i));
            i++;
        }

        res = new ArrayList<>(bpt.scanLeafNode(max, false, null));
        Collections.reverse(res);
        i = 0;
        for (Integer value : res) {
            assertEquals(value, new Integer(i));
            i++;
        }

        res = new ArrayList<>(bpt.scanLeafNode(5000000, true, null));
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
            public T0Operation(InMemoryBPlusTreeIndex<Integer, Integer> bpt) {
                this.bpt = bpt;
            }

            @Override
            public void run() {
                bpt.find(50, null);
            }
        }

        class T1Operation implements Runnable {
            private InMemoryBPlusTreeIndex<Integer, Integer> bpt;
            public T1Operation(InMemoryBPlusTreeIndex<Integer, Integer> bpt) {
                this.bpt = bpt;
            }

            @Override
            public void run() {
                bpt.insert(60, 60, null);
            }
        }

        InMemoryBPlusTreeIndex<Integer, Integer> bpt = new InMemoryBPlusTreeIndex<>(3);
        InMemoryBPlusTreeIndex<Integer, Integer>.Transaction txn = bpt.getTransactionManager().begin();

        bpt.insert(5, 100, txn);     // safe
        bpt.insert(9, 101, txn);     // safe
        bpt.insert(13, 102, txn);    // split in leaf
        bpt.insert(20, 103, txn);    // split in leaf
        bpt.insert(16, 104, txn);    // split in leaf and inner
        bpt.insert(50, 105, txn);    // split in leaf
        bpt.insert(1, 106, txn);     // safe
        bpt.insert(80, 107, txn);    // split in leaf and inner
        bpt.insert(8, 108, txn);     // split in leaf
        bpt.insert(12, 109, txn);    // safe
        bpt.insert(11, 110, txn);    // split in leaf and inner
        bpt.insert(17, 111, txn);    // safe
        bpt.insert(18, 112, txn);    // split in leaf
        bpt.insert(14, 113, txn);    // safe
        bpt.insert(15, 114, txn);    // split in leaf and inner
        bpt.insert(120, 115, txn);   // split in leaf
        bpt.insert(100, 116, txn);   // split in leaf and inner

        bpt.find(100, txn);

        bpt.traverseLeafNodes();

//        ExecutorService executorService = Executors.newCachedThreadPool();
//        executorService.submit(new T0Operation(bpt));
//        executorService.submit(new T1Operation(bpt));
//
//        // give detector enough time to finish detecting
//        TimeUnit.SECONDS.sleep(2);
//        executorService.shutdown();
    }
}
