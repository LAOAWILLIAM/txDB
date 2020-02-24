package test.index;

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
    @Test
    @SuppressWarnings("unchecked")
    public void serializableTest() {
        String dbFilePath = "/Users/williamhu/Documents/pitt/CS-2550/db/test.db";
        String logFilePath = dbFilePath.split("\\\\.")[0] + ".log";
        File dbFile = new File(dbFilePath);
        File logFile = new File(logFilePath);

        int bufferSize = 100;
        DiskManager diskManager = new DiskManager(dbFilePath);
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

        BufferManagerTest.deleteFile(dbFile);
        BufferManagerTest.deleteFile(logFile);
    }

    @Test
    @SuppressWarnings("unchecked")
    public void rootAsLeafNodeTest() {
        String dbFilePath = "/Users/williamhu/Documents/pitt/CS-2550/db/test.db";
        String logFilePath = dbFilePath.split("\\\\.")[0] + ".log";
        File dbFile = new File(dbFilePath);
        File logFile = new File(logFilePath);

        int bufferSize = 100;
        DiskManager diskManager = new DiskManager(dbFilePath);
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

        BufferManagerTest.deleteFile(dbFile);
        BufferManagerTest.deleteFile(logFile);
    }

    @Test
    @SuppressWarnings("unchecked")
    public void rootAsLeafNodeFirstSplitTest() {
        // TODO
        String dbFilePath = "/Users/williamhu/Documents/pitt/CS-2550/db/test.db";
        String logFilePath = dbFilePath.split("\\\\.")[0] + ".log";
        File dbFile = new File(dbFilePath);
        File logFile = new File(logFilePath);

        int bufferSize = 100;
        DiskManager diskManager = new DiskManager(dbFilePath);
        BufferManager bufferManager = new BufferManager(bufferSize, diskManager);

        BPlusTreeIndex<Integer, Integer> bpti = new BPlusTreeIndex<>(bufferManager, Config.INVALID_PAGE_ID, 3);
        bpti.insert(5, 100);
        bpti.insert(9, 101);
        bpti.insert(13, 102);
        assertEquals(bpti.find(5), new Integer(100));
        assertEquals(bpti.find(9), new Integer(101));
        assertEquals(bpti.find(13), new Integer(102));

        diskManager.close();

        BufferManagerTest.deleteFile(dbFile);
        BufferManagerTest.deleteFile(logFile);
    }
}
