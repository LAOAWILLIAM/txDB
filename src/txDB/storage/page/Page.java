package txDB.storage.page;

import txDB.Config;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class Page {
    private int pageId;
    private boolean isDirty;
    private int pinCount;
    private byte[] pageData;
    private int lsn;
    private ReadWriteLock readWriteLatch;
    private Lock readLatch;
    private Lock writeLatch;

    public Page() {
        this.pageId = Config.INVALID_PAGE_ID;
        this.pageData = new byte[Config.PAGE_SIZE];
        this.isDirty = false;
        this.pinCount = 0;
        this.lsn = Config.INVALID_LSN;
        this.readWriteLatch = new ReentrantReadWriteLock();
        this.readLatch = readWriteLatch.readLock();
        this.writeLatch = readWriteLatch.writeLock();
//        this.resetData();
    }

    public byte[] getPageData() {
        return this.pageData;
    }

    public int getPageId() {
        return this.pageId;
    }

    public int getPinCount() {
        return this.pinCount;
    }

    public boolean getIsDirty() {
        return this.isDirty;
    }

    public void setPageId(int pageId) {
        this.pageId = pageId;
    }

    public void setPinCount(int pinCount) {
        this.pinCount = pinCount;
    }

    public void incrementPinCount() {
        this.pinCount++;
    }

    public void decrementPinCount() {
        this.pinCount--;
    }

    public void setPageData(byte[] pageData) {
        if (pageData == null) this.resetData();
        else {
            if (pageData.length < Config.PAGE_SIZE) {
                ByteBuffer pageBuffer = ByteBuffer.wrap(new byte[Config.PAGE_SIZE]);
                pageBuffer.put(pageData);
                this.pageData = pageBuffer.array();
            } else this.pageData = pageData;
        }
    }

    public void setDirty(boolean isDirty) {
        this.isDirty = isDirty;
    }

    public void resetData() {
        Arrays.fill(this.pageData, (byte)0);
    }

    public void readLatch() {
        readLatch.lock();
    }

    public void readUnlatch() {
        readLatch.unlock();
    }

    public void writeLatch() {
        writeLatch.lock();
    }

    public void writeUnlatch() {
        writeLatch.unlock();
    }

    public int getLsn() {
        return lsn;
    }

    public void setLsn(int lsn) {
        this.lsn = lsn;
    }
}
