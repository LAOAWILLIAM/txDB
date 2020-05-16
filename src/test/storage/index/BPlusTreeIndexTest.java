package test.storage.index;

import org.junit.Test;
import static org.junit.Assert.*;

import test.buffer.BufferManagerTest;
import txDB.Config;
import txDB.buffer.BufferManager;
import txDB.storage.disk.DiskManager;
import txDB.storage.index.BPlusTreeIndex;
import txDB.storage.page.BPlusTreeLeafPageNode;
import txDB.storage.page.BPlusTreePageNode;
import txDB.storage.page.Page;

import java.io.*;
import java.util.ArrayList;
import java.util.Arrays;

public class BPlusTreeIndexTest {
    String dbName = "test";
    DiskManager diskManager = new DiskManager();

    public BPlusTreeIndexTest() throws IOException {
        diskManager.createFile(dbName);
        diskManager.useFile(dbName);
    }

    @Test
    @SuppressWarnings("unchecked")
    public void serializableTest() {
        int bufferSize = 100;
        BufferManager bufferManager = new BufferManager(bufferSize, diskManager);

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
    public void rootAsLeafNodeTest() throws IOException, ClassNotFoundException {
        int bufferSize = 100;
        BufferManager bufferManager = new BufferManager(bufferSize, diskManager);

        BPlusTreeIndex<Integer, Integer> bpti = new BPlusTreeIndex<>(bufferManager, Config.INVALID_PAGE_ID, 3);
        bpti.insert(3, 6);

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
    public void rootAsLeafNodeFirstSplitTest() throws IOException, ClassNotFoundException {
        int bufferSize = 100;
        BufferManager bufferManager = new BufferManager(bufferSize, diskManager);

        BPlusTreeIndex<Integer, Integer> bpti = new BPlusTreeIndex<>(bufferManager, Config.INVALID_PAGE_ID, 3);
        bpti.insert(5, 100);
        bpti.insert(9, 101);
        bpti.insert(13, 102);
        assertEquals(bpti.find(5), new Integer(100));
        assertEquals(bpti.find(9), new Integer(101));
        assertEquals(bpti.find(13), new Integer(102));

        diskManager.close();

        diskManager.dropFile(dbName);
    }

    @Test
    public void rootAsLeafNodeFirstSplitAndInsertTest() throws IOException, ClassNotFoundException {
        int bufferSize = 100;
        BufferManager bufferManager = new BufferManager(bufferSize, diskManager);

        BPlusTreeIndex<Integer, Integer> bpti = new BPlusTreeIndex<>(bufferManager, Config.INVALID_PAGE_ID, 3);
        bpti.insert(5, 100);
        bpti.insert(9, 101);
        bpti.insert(13, 102);
        bpti.insert(20, 103);
        assertEquals(bpti.find(5), new Integer(100));
        assertEquals(bpti.find(9), new Integer(101));
        assertEquals(bpti.find(13), new Integer(102));
        assertEquals(bpti.find(20), new Integer(103));

        bpti.insert(1, 104);
        assertEquals(bpti.find(1), new Integer(104));

        bpti.insert(10, 105);
        assertEquals(bpti.find(10), new Integer(105));

        diskManager.close();

        diskManager.dropFile(dbName);
    }

    @Test
    @SuppressWarnings("unchecked")
    public void internalNodeSplitTest() throws IOException, ClassNotFoundException {
        int bufferSize = 100;
        BufferManager bufferManager = new BufferManager(bufferSize, diskManager);

        BPlusTreeIndex<Integer, Integer> bpti = new BPlusTreeIndex<>(bufferManager, Config.INVALID_PAGE_ID, 3);
        bpti.insert(5, 100);
        bpti.insert(9, 101);
        bpti.insert(13, 102);
        bpti.insert(20, 103);
        assertEquals(bpti.find(5), new Integer(100));
        assertEquals(bpti.find(9), new Integer(101));
        assertEquals(bpti.find(13), new Integer(102));
        assertEquals(bpti.find(20), new Integer(103));

        bpti.insert(16, 104);
        assertEquals(bpti.find(16), new Integer(104));

        bpti.insert(50, 105);
        assertEquals(bpti.find(50), new Integer(105));

        bpti.insert(1, 106);
        assertEquals(bpti.find(1), new Integer(106));

        bpti.insert(80, 107);
        assertEquals(bpti.find(80), new Integer(107));

        bpti.insert(8, 108);
        assertEquals(bpti.find(8), new Integer(108));

        bpti.insert(12, 109);
        assertEquals(bpti.find(12), new Integer(109));

        bpti.insert(11, 110);
        assertEquals(bpti.find(11), new Integer(110));

        bpti.insert(17, 111);
        assertEquals(bpti.find(17), new Integer(111));

        bpti.insert(18, 112);
        assertEquals(bpti.find(18), new Integer(112));

        bpti.insert(14, 113);
        assertEquals(bpti.find(14), new Integer(113));

        bpti.insert(15, 114);
        assertEquals(bpti.find(15), new Integer(114));

        bpti.insert(120, 115);
        assertEquals(bpti.find(120), new Integer(115));

        bpti.insert(100, 116);
        assertEquals(bpti.find(100), new Integer(116));

        bpti.traverseAllNodes();

        bufferManager.flushAllPages();

        diskManager.close();

        diskManager.dropFile(dbName);
    }

    @Test
    public void insertScaleTest() throws IOException, ClassNotFoundException {
        int bufferSize = 100000;
        BufferManager bufferManager = new BufferManager(bufferSize, diskManager);

        BPlusTreeIndex<Integer, Integer> bpti = new BPlusTreeIndex<>(bufferManager, Config.INVALID_PAGE_ID, 100);

        int max = 100000, i;
        for(i = 0; i < max; i++) {
            bpti.insert(i, i);
            assertEquals(bpti.find(i), new Integer(i));
        }
    }

    @Test
    public void insertPersistScaleTest() throws IOException, ClassNotFoundException {
        int bufferSize = 100000;
        BufferManager bufferManager = new BufferManager(bufferSize, diskManager);

        BPlusTreeIndex<Integer, Integer> bpti = new BPlusTreeIndex<>(bufferManager, Config.INVALID_PAGE_ID, 100);

        int max = 100000, i;
        for(i = 0; i < max; i++) {
            bpti.insert(i, i);
        }

        bufferManager.flushAllPages();

        for(i = 0; i < max; i++) {
            assertEquals(bpti.find(i), new Integer(i));
        }
    }

    @Test
    public void traverseLeafNodesTest() throws IOException, ClassNotFoundException {
        int bufferSize = 100;
        BufferManager bufferManager = new BufferManager(bufferSize, diskManager);

        BPlusTreeIndex<Integer, Integer> bpti = new BPlusTreeIndex<>(bufferManager, Config.INVALID_PAGE_ID, 3);

        bpti.insert(5, 100);
        bpti.insert(9, 101);
        bpti.insert(13, 102);
        bpti.insert(20, 103);
        assertEquals(bpti.find(5), new Integer(100));
        assertEquals(bpti.find(9), new Integer(101));
        assertEquals(bpti.find(13), new Integer(102));
        assertEquals(bpti.find(20), new Integer(103));

        bpti.insert(16, 104);
        assertEquals(bpti.find(16), new Integer(104));

        bpti.insert(50, 105);
        assertEquals(bpti.find(50), new Integer(105));

        bpti.insert(1, 106);
        assertEquals(bpti.find(1), new Integer(106));

        bpti.insert(80, 107);
        assertEquals(bpti.find(80), new Integer(107));

        bpti.insert(8, 108);
        assertEquals(bpti.find(8), new Integer(108));

        bpti.insert(12, 109);
        assertEquals(bpti.find(12), new Integer(109));

        bpti.insert(11, 110);
        assertEquals(bpti.find(11), new Integer(110));

        bpti.insert(17, 111);
        assertEquals(bpti.find(17), new Integer(111));

        bpti.insert(18, 112);
        assertEquals(bpti.find(18), new Integer(112));

        bpti.insert(14, 113);
        assertEquals(bpti.find(14), new Integer(113));

        bpti.insert(15, 114);
        assertEquals(bpti.find(15), new Integer(114));

        bpti.insert(120, 115);
        assertEquals(bpti.find(120), new Integer(115));

        bpti.insert(100, 116);
        assertEquals(bpti.find(100), new Integer(116));

        bpti.traverseLeafNodes();
    }

    @Test
    public void deleteWithRedistributeAndMergeTest() throws IOException, ClassNotFoundException {
        int bufferSize = 100;
        BufferManager bufferManager = new BufferManager(bufferSize, diskManager);

        BPlusTreeIndex<Integer, Integer> bpti = new BPlusTreeIndex<>(bufferManager, Config.INVALID_PAGE_ID, 3);

        bpti.insert(5, 100);
        bpti.insert(9, 101);
        bpti.insert(13, 102);
        bpti.insert(20, 103);
        bpti.insert(30, 104);
        assertEquals(bpti.find(5), new Integer(100));
        assertEquals(bpti.find(9), new Integer(101));
        assertEquals(bpti.find(13), new Integer(102));
        assertEquals(bpti.find(20), new Integer(103));
        assertEquals(bpti.find(30), new Integer(104));

//        bpti.traverseLeafNodes();

        bpti.delete(13);
        assertNull(bpti.find(13));
//        bpti.traverseAllNodes();

        bpti.delete(20);
        assertNull(bpti.find(20));
//        bpti.traverseAllNodes();

        assertEquals(bpti.find(5), new Integer(100));
        assertEquals(bpti.find(9), new Integer(101));
        assertEquals(bpti.find(30), new Integer(104));

        bpti.traverseLeafNodes();
    }

    @Test
    public void deleteScaleTest() throws IOException, ClassNotFoundException {
        int bufferSize = 10000;
        BufferManager bufferManager = new BufferManager(bufferSize, diskManager);

        BPlusTreeIndex<Integer, Integer> bpti = new BPlusTreeIndex<>(bufferManager, Config.INVALID_PAGE_ID, 200);

        int max = 100000, i;
        for(i = 0; i < max; i++) {
            bpti.insert(i, i);
//            assertEquals(bpti.find(i), new Integer(i));
        }

        for(i = 0; i < max; i++) {
            bpti.delete(i);
            assertNull(bpti.find(i));
            if (i < max - 1)
                assertEquals(bpti.find(i + 1), new Integer(i + 1));
        }
    }
}
