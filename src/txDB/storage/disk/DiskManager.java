package txDB.storage.disk;

import txDB.Config;

import java.io.*;
import java.util.concurrent.atomic.*;
import java.nio.ByteBuffer;

public class DiskManager {

    private FileInputStream dbFileRead;
    private FileOutputStream dbFileWrite;
    private String dbFilePath;
    private FileInputStream logFileRead;
    private FileOutputStream logFileWrite;
    private String logFilePath;
    private AtomicInteger nextPageId;

    /**
     *
     * @param dbFilePath
     */
    public DiskManager(String dbFilePath) {
        // TODO: nextPageId should be based on persisted id
        this.nextPageId = new AtomicInteger(0);

        this.dbFilePath = dbFilePath;
        this.logFilePath = dbFilePath.split("\\\\.")[0] + ".log";

        try {
            this.dbFileWrite = new FileOutputStream(this.dbFilePath);
            this.dbFileRead = new FileInputStream(this.dbFilePath);
            this.logFileWrite = new FileOutputStream(this.logFilePath);
            this.logFileRead = new FileInputStream(this.logFilePath);
        } catch (FileNotFoundException e) {
            System.out.println(e);
        }
    }

    /**
     *
     * @param pageId
     * @return
     */
    public byte[] readPage(int pageId) {
        ByteBuffer pageData = ByteBuffer.allocate(Config.PAGE_SIZE);
        int offset = pageId * Config.PAGE_SIZE;
        try {
//            if (this.dbFileRead.read(pageData, offset, Config.PAGE_SIZE) == -1)
            if (this.dbFileRead.getChannel().read(pageData, offset) != -1)
                return pageData.array();
        } catch (IOException e) {
            System.out.println(e);
        }

        return null;
    }

    /**
     *
     * @param pageId
     * @param pageData
     */
    public void writePage(int pageId, byte[] pageData) {
        int offset = pageId * Config.PAGE_SIZE;
        try {
            this.dbFileWrite.getChannel().write(ByteBuffer.wrap(pageData), offset);
            // there is no need to use the method `flush`,
            // as there is no buffered data in memory for OutputStream.
        } catch (IOException e) {
            System.out.println(e);
        }
    }

    /**
     *
     */
    public void readLog() {
        // TODO
    }

    /**
     *
     */
    public void writeLog() {
        // TODO
    }

    /**
     *
     * @return
     */
    public int allocatePage() {
        int res = this.nextPageId.get();
        this.nextPageId.incrementAndGet();
        return res;
    }

    public void revokeAllocatedPage() {
        this.nextPageId.decrementAndGet();
    }

    /**
     *
     * @param pageId
     */
    public void deAllocatePage(int pageId) {
        // TODO
    }

    /**
     * Close streams
     */
    public void close() {
        try {
            this.dbFileRead.close();
            this.dbFileWrite.close();
            this.logFileRead.close();
            this.logFileWrite.close();
        } catch (IOException e) {
            System.out.println(e);
        }
    }
}
