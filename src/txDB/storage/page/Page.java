package txDB.storage.page;

import txDB.Config;

import java.nio.ByteBuffer;
import java.util.Arrays;

public class Page {
    private int pageId;
    private boolean isDirty;
    private int pinCount;
    private byte[] pageData;

    public Page() {
        this.pageId = Config.INVALID_PAGE_ID;
        this.pageData = new byte[Config.PAGE_SIZE];
        this.isDirty = false;
        this.pinCount = 0;
        this.resetData();
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

    private void resetData() {
        Arrays.fill(this.pageData, (byte)0);
    }
}
