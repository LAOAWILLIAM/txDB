package test.index;

import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.*;

import txDB.storage.index.InMemoryBPlusTreeIndex;

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
        assertNull(bpt.find(5));

        bpt.insert(5, 100);
        bpt.insert(9, 101);
        assertEquals(bpt.find(5), new Integer(100));
        assertEquals(bpt.find(9), new Integer(101));
    }

    @Test
    public void rootAsLeafNodeFirstSplitTest() {
        InMemoryBPlusTreeIndex<Integer, Integer> bpt = new InMemoryBPlusTreeIndex<>(3);
        assertNull(bpt.find(5));

        bpt.insert(5, 100);
        bpt.insert(9, 101);
        bpt.insert(13, 102);
        assertEquals(bpt.find(5), new Integer(100));
        assertEquals(bpt.find(9), new Integer(101));
        assertEquals(bpt.find(13), new Integer(102));
    }

    @Test
    public void rootAsLeafNodeFirstSplitAndInsertTest() {
        InMemoryBPlusTreeIndex<Integer, Integer> bpt = new InMemoryBPlusTreeIndex<>(3);
        assertNull(bpt.find(5));

        bpt.insert(5, 100);
        bpt.insert(9, 101);
        bpt.insert(13, 102);
        bpt.insert(20, 103);
        assertEquals(bpt.find(5), new Integer(100));
        assertEquals(bpt.find(9), new Integer(101));
        assertEquals(bpt.find(13), new Integer(102));
        assertEquals(bpt.find(20), new Integer(103));

        bpt.insert(1, 104);
        assertEquals(bpt.find(1), new Integer(104));

        bpt.insert(10, 105);
        assertEquals(bpt.find(10), new Integer(105));
    }

    @Test
    public void internalNodeSplitTest() {
        InMemoryBPlusTreeIndex<Integer, Integer> bpt = new InMemoryBPlusTreeIndex<>(3);
        assertNull(bpt.find(5));

        bpt.insert(5, 100);
        bpt.insert(9, 101);
        bpt.insert(13, 102);
        bpt.insert(20, 103);
        assertEquals(bpt.find(5), new Integer(100));
        assertEquals(bpt.find(9), new Integer(101));
        assertEquals(bpt.find(13), new Integer(102));
        assertEquals(bpt.find(20), new Integer(103));

        bpt.insert(16, 104);
        assertEquals(bpt.find(16), new Integer(104));

        bpt.insert(50, 105);
        assertEquals(bpt.find(50), new Integer(105));

        bpt.insert(1, 106);
        assertEquals(bpt.find(1), new Integer(106));

        bpt.insert(80, 107);
        assertEquals(bpt.find(80), new Integer(107));

        bpt.insert(8, 108);
        assertEquals(bpt.find(8), new Integer(108));

        bpt.insert(12, 109);
        assertEquals(bpt.find(12), new Integer(109));

        bpt.insert(11, 110);
        assertEquals(bpt.find(11), new Integer(110));

        bpt.insert(17, 111);
        assertEquals(bpt.find(17), new Integer(111));

        bpt.insert(18, 112);
        assertEquals(bpt.find(18), new Integer(112));

        bpt.insert(14, 113);
        assertEquals(bpt.find(14), new Integer(113));

        bpt.insert(15, 114);
        assertEquals(bpt.find(15), new Integer(114));

        bpt.insert(120, 115);
        assertEquals(bpt.find(120), new Integer(115));

        bpt.insert(100, 116);
        assertEquals(bpt.find(100), new Integer(116));

        bpt.traverseAllNodes();
    }

    @Test
    public void insertScaleTest() {
        InMemoryBPlusTreeIndex<Integer, Integer> bpt = new InMemoryBPlusTreeIndex<>(10);
        assertNull(bpt.find(5));

        int max = 10000000, i;
        for(i = 0; i < max; i++) {
            bpt.insert(i, i);
            assertEquals(bpt.find(i), new Integer(i));
        }

//        bpt.traverseAllNodes();
    }

    @Test
    public void traverseLeafNodesTest() {
        InMemoryBPlusTreeIndex<Integer, Integer> bpt = new InMemoryBPlusTreeIndex<>(3);
        assertNull(bpt.find(5));

        bpt.insert(5, 100);
        bpt.insert(9, 101);
        bpt.insert(13, 102);
        bpt.insert(20, 103);
        assertEquals(bpt.find(5), new Integer(100));
        assertEquals(bpt.find(9), new Integer(101));
        assertEquals(bpt.find(13), new Integer(102));
        assertEquals(bpt.find(20), new Integer(103));

        bpt.insert(16, 104);
        assertEquals(bpt.find(16), new Integer(104));

        bpt.insert(50, 105);
        assertEquals(bpt.find(50), new Integer(105));

        bpt.insert(1, 106);
        assertEquals(bpt.find(1), new Integer(106));

        bpt.insert(80, 107);
        assertEquals(bpt.find(80), new Integer(107));

        bpt.insert(8, 108);
        assertEquals(bpt.find(8), new Integer(108));

        bpt.insert(12, 109);
        assertEquals(bpt.find(12), new Integer(109));

        bpt.insert(11, 110);
        assertEquals(bpt.find(11), new Integer(110));

        bpt.insert(17, 111);
        assertEquals(bpt.find(17), new Integer(111));

        bpt.insert(18, 112);
        assertEquals(bpt.find(18), new Integer(112));

        bpt.insert(14, 113);
        assertEquals(bpt.find(14), new Integer(113));

        bpt.insert(15, 114);
        assertEquals(bpt.find(15), new Integer(114));

        bpt.insert(120, 115);
        assertEquals(bpt.find(120), new Integer(115));

        bpt.insert(100, 116);
        assertEquals(bpt.find(100), new Integer(116));

        bpt.traverseLeafNodes();
    }

    @Test
    public void deleteWithoutRedistributeAndMergeTest() {
        InMemoryBPlusTreeIndex<Integer, Integer> bpt = new InMemoryBPlusTreeIndex<>(5);
        assertNull(bpt.find(5));

        bpt.insert(5, 100);
        bpt.insert(9, 101);
        bpt.insert(13, 102);
        bpt.insert(20, 103);
        bpt.insert(30, 104);
        assertEquals(bpt.find(5), new Integer(100));
        assertEquals(bpt.find(9), new Integer(101));
        assertEquals(bpt.find(13), new Integer(102));
        assertEquals(bpt.find(20), new Integer(103));
        assertEquals(bpt.find(30), new Integer(104));

        bpt.delete(13);
        assertNull(bpt.find(13));

        assertEquals(bpt.find(20), new Integer(103));
    }

    @Test
    public void deleteWithRedistributeAndMergeTest() {
        InMemoryBPlusTreeIndex<Integer, Integer> bpt = new InMemoryBPlusTreeIndex<>(3);
        assertNull(bpt.find(5));

        bpt.insert(5, 100);
        bpt.insert(9, 101);
        bpt.insert(13, 102);
        bpt.insert(20, 103);
        bpt.insert(30, 104);
        assertEquals(bpt.find(5), new Integer(100));
        assertEquals(bpt.find(9), new Integer(101));
        assertEquals(bpt.find(13), new Integer(102));
        assertEquals(bpt.find(20), new Integer(103));
        assertEquals(bpt.find(30), new Integer(104));

//        bpt.traverseAllNodes();

        bpt.delete(13);
        assertNull(bpt.find(13));
        bpt.traverseAllNodes();

        bpt.delete(20);
        assertNull(bpt.find(20));

        assertEquals(bpt.find(5), new Integer(100));
        assertEquals(bpt.find(9), new Integer(101));
        assertEquals(bpt.find(30), new Integer(104));

//        bpt.traverseLeafNodes();
    }

    @Test
    public void deleteScaleTest() {
        InMemoryBPlusTreeIndex<Integer, Integer> bpt = new InMemoryBPlusTreeIndex<>(3);
        assertNull(bpt.find(5));

        int max = 1000000, i;
        for(i = 0; i < max; i++) {
            bpt.insert(i, i);
            assertEquals(bpt.find(i), new Integer(i));
        }

        for(i = 0; i < max; i++) {
            bpt.delete(i);
            assertNull(bpt.find(i));
//            bpt.traverseAllNodes();
//            System.out.println();
            if (i < max - 1)
                assertEquals(bpt.find(i + 1), new Integer(i + 1));
        }
    }
}
