package test.index;

import org.junit.Test;
import static org.junit.Assert.*;

import test.buffer.BufferManagerTest;
import txDB.buffer.BufferManager;
import txDB.storage.disk.DiskManager;
import txDB.storage.page.BPlusTreePageNode;
import txDB.storage.page.Page;

import java.io.*;

public class BPlusTreeIndexTest {
    @Test
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
            BPlusTreePageNode bPlusTreePageNode = new BPlusTreePageNode();
            bPlusTreePageNode.setPageId(page0.getPageId());
            bPlusTreePageNode.setIndexPageType(BPlusTreePageNode.IndexPageType.LEAFPAGE);

            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            ObjectOutput out = new ObjectOutputStream(bos);
            out.writeObject(bPlusTreePageNode);
            byte[] pageData = bos.toByteArray();
            System.out.println("pageData length: " + pageData.length);
            page0.setPageData(pageData);
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
    public void indexTest() {
        // TODO
    }
}
