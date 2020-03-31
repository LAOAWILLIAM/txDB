package test.buffer;

import txDB.Config;
import txDB.buffer.BufferManager;
import txDB.storage.disk.DiskManager;
import txDB.storage.page.Page;
import txDB.storage.page.TablePage;

import org.junit.Test;
import static org.junit.Assert.*;
import java.io.File;
import java.nio.ByteBuffer;

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

        byte[] hello = "hello".getBytes();
        page0.setPageData(hello);
        ByteBuffer byteBuffer = ByteBuffer.allocate(Config.PAGE_SIZE);
        byteBuffer.put(hello);
        assertEquals(new String(byteBuffer.array()), new String(page0.getPageData()));

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
        assertEquals(new String(byteBuffer.array()), new String(page0.getPageData()));

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
            TablePage tablePage = new TablePage(page);
            bufferManager.replacePage(tablePage);
            tablePage.setPrevPageId(i - 1);
            tablePage.setNextPageId(i + 1);
        }

        for (i = 0; i < bufferSize; i++) {
            assertTrue(bufferManager.unpinPage(i, true));
        }

        for (i = 0; i < bufferSize; i++) {
            assertTrue(bufferManager.flushPage(i));
        }

        page = bufferManager.fetchPage(5000);
//        System.out.println(new String(page5000.getPageData()));
        TablePage page5000 = new TablePage(page);
        bufferManager.replacePage(page5000);
        assertEquals(page5000.getPrevPageId(), 4999);
        assertEquals(page5000.getNextPageId(), 5001);

        diskManager.close();

        deleteFile(dbFile);
        deleteFile(logFile);
    }

    @Test
    public void multithreadTest() {
        class BmThread1 extends Thread {
            private BufferManager bufferManager;

            private BmThread1(BufferManager bufferManager) {
                this.bufferManager = bufferManager;
            }

            public void run() {
                this.bufferManager.fetchPage(1);
                this.bufferManager.unpinPage(1, false);
            }
        }

        class BmThread2 extends Thread {

            private BufferManager bufferManager;

            private BmThread2(BufferManager bufferManager) {
                this.bufferManager = bufferManager;
            }

            public void run() {
//                this.bufferManager.
            }
        }

        class BmThread3 extends Thread {

            private BufferManager bufferManager;

            private BmThread3(BufferManager bufferManager) {
                this.bufferManager = bufferManager;
            }

            public void run() {
//                this.bufferManager.
            }
        }

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

        BmThread1 bmThread1 = new BmThread1(bufferManager);
        BmThread2 bmThread2 = new BmThread2(bufferManager);
        BmThread3 bmThread3 = new BmThread3(bufferManager);

        bmThread1.start();
        bmThread2.start();
        bmThread3.start();

        try {
            bmThread1.join();
            bmThread2.join();
            bmThread3.join();
        } catch (Exception e) {
            e.printStackTrace();
        }

        diskManager.close();

        deleteFile(dbFile);
        deleteFile(logFile);

    }

    /**
     * common function to delete file while testing
     * @param file
     */
    public static void deleteFile(File file) {
        if(file.delete()) {
            System.out.println("File deleted successfully");
        } else {
            System.out.println("Failed to delete the file");
        }
    }
}
