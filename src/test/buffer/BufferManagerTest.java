package test.buffer;

import txDB.buffer.BufferManager;
import txDB.storage.disk.DiskManager;
import txDB.storage.page.Page;
import txDB.storage.page.TablePage;

import org.junit.Test;
import static org.junit.Assert.*;
import java.io.File;

public class BufferManagerTest {
    @Test
    public void bufferManagerTest1() {
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

        page0.setPageData("hello".getBytes());
        assertTrue("hello".equals(new String(page0.getPageData())));

        int i;
        for (i = 1; i < bufferSize; i++) {
            assertNotNull(bufferManager.newPage());
        }

        for (i = bufferSize; i < bufferSize * 2; i++) {
            assertNull(bufferManager.newPage());
        }

        for (i = 0; i < 5; ++i) {
            assertTrue(bufferManager.unpinPage(i, true));
        }
        for (i = 0; i < 4; ++i) {
            assertNotNull(bufferManager.newPage());
        }

        page0 = bufferManager.fetchPage(0);
        assertNotNull(page0);
        System.out.println(new String(page0.getPageData()));
//        assertTrue("hello".equals(new String(page0.getPageData())));

        assertTrue(bufferManager.unpinPage(0, true));
        assertNotNull(bufferManager.newPage());
        assertNull(bufferManager.fetchPage(0));

        diskManager.close();

        deleteFile(dbFile);
        deleteFile(logFile);
    }

    @Test
    public void bufferManagerTest2() {
        String dbFilePath = "/Users/williamhu/Documents/pitt/CS-2550/db/test.db";
        String logFilePath = dbFilePath.split("\\\\.")[0] + ".log";
        File dbFile = new File(dbFilePath);
        File logFile = new File(logFilePath);

        int bufferSize = 100000;
        DiskManager diskManager = new DiskManager(dbFilePath);
        BufferManager bufferManager = new BufferManager(bufferSize, diskManager);

        int i;
        Page page;
        for (i = 0; i < bufferSize; i++) {
            assertNotNull(page = bufferManager.newPage());
            /**
             * `instanceof` needed to avoid `ClassCastException`
             */
            if (page instanceof TablePage) {
                TablePage tablePage = (TablePage) page;
                tablePage.setPrevPageId(i - 1);
                tablePage.setNextPageId(i + 1);
            }
        }

        for (i = 0; i < bufferSize; i++) {
            assertTrue(bufferManager.unpinPage(i, true));
        }

        for (i = 0; i < bufferSize; i++) {
            assertTrue(bufferManager.flushPage(i));
        }

        page = bufferManager.fetchPage(5000);
//        System.out.println(new String(page5000.getPageData()));
        /**
         * `instanceof` needed to avoid `ClassCastException`
         */
        if (page instanceof TablePage) {
            TablePage page5000 = (TablePage) page;
            assertEquals(page5000.getPrevPageId(), 4999);
            assertEquals(page5000.getNextPageId(), 5001);
        }

        diskManager.close();

        deleteFile(dbFile);
        deleteFile(logFile);
    }

    private void deleteFile(File file) {
        if(file.delete()) {
            System.out.println("File deleted successfully");
        } else {
            System.out.println("Failed to delete the file");
        }
    }
}