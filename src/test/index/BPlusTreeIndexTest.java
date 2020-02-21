package test.index;

import org.junit.Test;
import static org.junit.Assert.*;

import test.buffer.BufferManagerTest;
import txDB.buffer.BufferManager;
import txDB.storage.disk.DiskManager;
import txDB.storage.page.BPlusTreePage;
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
            BPlusTreePage bPlusTreePage = new BPlusTreePage();
            bPlusTreePage.setPageId(page0.getPageId());
            bPlusTreePage.setIndexPageType(BPlusTreePage.IndexPageType.LEAFPAGE);

            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            ObjectOutput out = new ObjectOutputStream(bos);
            out.writeObject(bPlusTreePage);
            page0.setPageData(bos.toByteArray());
            bufferManager.flushPage(page0.getPageId());

            page0 = bufferManager.fetchPage(0);
            ByteArrayInputStream bis = new ByteArrayInputStream(page0.getPageData());
            ObjectInputStream in = new ObjectInputStream(bis);
            bPlusTreePage = (BPlusTreePage) in.readObject();
            assertEquals(bPlusTreePage.getPageId(), 0);
            assertEquals(bPlusTreePage.getIndexPageType(), BPlusTreePage.IndexPageType.LEAFPAGE);

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
