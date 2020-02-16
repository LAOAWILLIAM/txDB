package txDB.buffer;

import txDB.storage.disk.DiskManager;
import txDB.storage.page.Page;

public class BufferManager {
    private LRUBufferPool lruBufferPool;
    private DiskManager diskManager;

    /**
     *
     * @param bufferSize
     * @param diskManager
     */
    public BufferManager(int bufferSize, DiskManager diskManager) {
        this.lruBufferPool = new LRUBufferPool(bufferSize, diskManager);
        this.diskManager = diskManager;
    }

    /**
     *
     * @param bufferSize
     * @param loadFactor
     * @param diskManager
     */
    public BufferManager(int bufferSize, float loadFactor, DiskManager diskManager) {
        this.lruBufferPool = new LRUBufferPool(bufferSize, loadFactor, diskManager);
        this.diskManager = diskManager;
    }

    /**
     *
     * @param pageId
     * @return
     */
    public Page fetchPage(int pageId) {
        synchronized (this) {
            Page requestPage;
            if ((requestPage = this.lruBufferPool.get(pageId)) != null) {
                requestPage.incrementPinCount();
                return requestPage;
            }

            byte[] pageData = this.diskManager.readPage(pageId);
            if (pageData == null) return null;

            requestPage = new Page();
            requestPage.setPageData(pageData);
            requestPage.setPageId(pageId);
            requestPage.setPinCount(1);
            requestPage.setDirty(false);

            if (!this.lruBufferPool.put(pageId, requestPage)) return null;

            return requestPage;
        }
    }

    /**
     *
     * @param pageId
     * @param isDirty
     * @return
     */
    public boolean unpinPage(int pageId, boolean isDirty) {
        synchronized (this) {
            Page requestPage;
            if ((requestPage = this.lruBufferPool.get(pageId)) == null) return false;
            if (requestPage.getPinCount() <= 0) return false;

            requestPage.setPinCount(0);
            requestPage.setDirty(isDirty);

            return true;
        }
    }

    /**
     *
     */
    public Page newPage() {
        // Allocating next page is thread safe based on atomicInteger
        int pageId = this.diskManager.allocatePage();

        synchronized (this) {

            // The following case can be ignored,
            // as checking whether all pinned will take place in `lruBufferPool.put` method
            // if (this.lruBufferPool.allPinned()) return null;

            Page requestPage = new Page();
            requestPage.setPageId(pageId);
            requestPage.setPinCount(1);

            if (!this.lruBufferPool.put(pageId, requestPage)) return null;

            return requestPage;
        }
    }

    /**
     *
     * @param pageId
     * @return
     */
    public boolean flushPage(int pageId) {
        synchronized (this) {
            Page requestPage = this.lruBufferPool.get(pageId);
            if (requestPage == null) return false;
            if (requestPage.getPinCount() != 0) return false;

            if (requestPage.getIsDirty())
                // TODO: writing page one by one is not efficient,
                //  batch writing should be used to do this
                this.diskManager.writePage(pageId, requestPage.getPageData());

            this.lruBufferPool.delete(pageId);

            return true;
        }
    }

    /**
     *
     */
    public void fulshAllPages() {
        // TODO
    }

    /**
     *
     * @param pageId
     * @return
     */
    public boolean deletePage(int pageId) {
        synchronized (this) {
            Page requestPage = this.lruBufferPool.get(pageId);
            if (requestPage == null) return false;
            if (requestPage.getPinCount() != 0) return false;

            this.lruBufferPool.delete(pageId);
            this.diskManager.deAllocatePage(pageId);

            return true;
        }
    }
}
