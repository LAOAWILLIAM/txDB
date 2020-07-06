package txDB.storage.disk;

import txDB.Config;

import java.io.*;
import java.util.concurrent.atomic.*;
import java.nio.ByteBuffer;

public class DiskManager {

    private final String dbRootPath = "/Users/williamhu/Documents/pitt/CS-2550/db/";
    private FileInputStream dbFileRead;
    private FileOutputStream dbFileWrite;
    private FileInputStream logFileRead;
    private FileOutputStream logFileWrite;
    private File logFile;
    private AtomicInteger nextPageId;

    public DiskManager() {
    }

    public void createFile(String dbName) throws IOException {
        String dbFilePath = this.dbRootPath + dbName + ".db";
        String logFilePath = dbFilePath.split("\\\\.")[0] + ".log";
        File dbFile = new File(dbFilePath);
        File logFile = new File(logFilePath);

        boolean res1 = false, res2 = false;
        if (!dbFile.exists() || !dbFile.isFile()) {
            res1 = dbFile.createNewFile();
            res2 = logFile.createNewFile();
        }
//        if (!logFile.exists() || !logFile.isFile())
//            res2 = logFile.createNewFile();

        if (res1) {
            System.out.println("Create " + dbName);
            if (res2) {
                System.out.println("Create log file for " + dbName);
            }
        } else {
            System.out.println(dbName + " already exists");
        }
//        if (res2) {
//            System.out.println("Create log file for " + dbName);
//        }
    }

    public void dropFile(String dbName) {
        String dbFilePath = this.dbRootPath + dbName + ".db";
        String logFilePath = dbFilePath.split("\\\\.")[0] + ".log";
        File dbFile = new File(dbFilePath);
        File logFile = new File(logFilePath);

        if(dbFile.delete()) {
            System.out.println("Drop " + dbName);
            if (logFile.delete()) {
                System.out.println("Drop log file for " + dbName);
            } else {
                System.out.println("Failed to drop log file for " + dbName);
            }
        } else {
            System.out.println("Failed to drop " + dbName);
        }
//        if (logFile.delete()) {
//            System.out.println("Drop log file for " + dbName);
//        } else {
//            System.out.println("Failed to drop log file for " + dbName);
//        }
    }

    public void useFile(String dbName) throws FileNotFoundException {
        String dbFilePath = this.dbRootPath + dbName + ".db";
        String logFilePath = dbFilePath.split("\\\\.")[0] + ".log";
        File dbFile = new File(dbFilePath);
        logFile = new File(logFilePath);

        if (!dbFile.exists() || !dbFile.isFile()) {
            throw new FileNotFoundException();
        }

        if (!logFile.exists() || !logFile.isFile()) {
            throw new FileNotFoundException();
        }

        // nextPageId is based on persisted database file length
        this.nextPageId = new AtomicInteger((int) (dbFile.length() / Config.PAGE_SIZE));

        try {
            this.dbFileWrite = new FileOutputStream(dbFilePath, true);
            this.dbFileRead = new FileInputStream(dbFilePath);
            this.logFileWrite = new FileOutputStream(logFilePath, true);
            this.logFileRead = new FileInputStream(logFilePath);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
    }

    /**
     * read page from corresponding offset based on pageId
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
            e.printStackTrace();
        }

        return null;
    }

    /**
     * write page to corresponding offset based on pageId
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
            e.printStackTrace();
        }
    }

    /**
     * sequence read when reading the log file
     */
    public byte[] readLog(int size, int offset) {
        ByteBuffer logData = ByteBuffer.allocate(size);
        try {
            if (this.logFileRead.getChannel().read(logData, offset) != -1)
                return logData.array();
        } catch (IOException e) {
            e.printStackTrace();
        }

        return null;
    }

    /**
     * append to the end of the log file
     * @param logData
     */
    public int writeLog(byte[] logData, boolean whetherCheckpoint) {
        int logFileLength = -1;
        try {
            if (whetherCheckpoint) {
                logFileLength = (int) logFile.length();
            }
            this.logFileWrite.write(logData);
        } catch (IOException e) {
            e.printStackTrace();
        }

        return logFileLength;
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
            e.printStackTrace();
        }
    }
}
