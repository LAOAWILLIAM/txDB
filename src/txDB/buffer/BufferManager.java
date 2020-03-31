package txDB.buffer;

import txDB.storage.disk.DiskManager;
import txDB.storage.page.Page;
import txDB.storage.page.TablePage;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.ListIterator;
import java.util.Set;

public class BufferManager {
    private LRUBufferPool lruBufferPool;
    private DiskManager diskManager;
    private LinkedList<Page> freeList;

    /**
     *
     * @param bufferSize
     * @param diskManager
     */
    public BufferManager(int bufferSize, DiskManager diskManager) {
        this.lruBufferPool = new LRUBufferPool(bufferSize, diskManager);
        this.diskManager = diskManager;
        this.freeList = new LinkedList<>();
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
        this.freeList = new LinkedList<>();
    }

    /**
     *
     * @param pageId
     * @return
     */
    public Page fetchPage(int pageId) {
        synchronized (this) {
            Page requestPage;
            if ((requestPage = this.lruBufferPool.get(pageId, true)) != null) {
                if (requestPage.getIsDirty())
                    this.diskManager.writePage(pageId, requestPage.getPageData());
                requestPage.incrementPinCount();
                return requestPage;
            }

            byte[] pageData = this.diskManager.readPage(pageId);
            if (pageData == null) {
//                System.out.println("1 unable to fetch page " + pageId);
                return null;
            }

//            requestPage = this.findUnusedPage();
//
//            if (requestPage == null) {
//                requestPage = new Page();
//            }

            requestPage = new Page();
            requestPage.setPageData(pageData);
            requestPage.setPageId(pageId);
            requestPage.setPinCount(1);
            requestPage.setDirty(false);

            if (!this.lruBufferPool.put(pageId, requestPage)) {
//                System.out.println("2 unable to fetch page " + pageId);
                return null;
            }

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
            if ((requestPage = this.lruBufferPool.get(pageId, false)) == null) return false;
            if (requestPage.getPinCount() <= 0) return false;

//            System.out.println("enter effective unpin area in page " + pageId + ", dirty: " + isDirty);

            requestPage.decrementPinCount();
//            requestPage.setPinCount(0);
            requestPage.setDirty(isDirty);

            return true;
        }
    }

    /**
     *
     */
    public Page newPage()  {
        // Allocating next page is thread safe based on atomicInteger
        int pageId = this.diskManager.allocatePage();

        synchronized (this) {

            // The following case can be ignored,
            // as checking whether all pinned will take place in `lruBufferPool.put` method
            // if (this.lruBufferPool.allPinned()) return null;

            Page requestPage = this.findUnusedPage();

            if (requestPage == null) {
                requestPage = new Page();
                requestPage.setPageId(pageId);
            } else {
                pageId = requestPage.getPageId();
            }

            requestPage.setPinCount(1);

            if (!this.lruBufferPool.put(pageId, requestPage)) {
//                System.out.println("unable to new page");
                requestPage.setPinCount(0);
                this.diskManager.revokeAllocatedPage();
                return null;
            }

            this.freeList.poll();

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
            Page requestPage = this.lruBufferPool.get(pageId, false);
            if (requestPage == null) return false;
            if (requestPage.getPinCount() != 0) {
                System.out.println("pinCount: " + requestPage.getPinCount());
                return false;
            }

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
    public void flushAllPages() {
        synchronized (this) {
            ArrayList<Integer> flushList = new ArrayList<>();
            for (int pageId: this.lruBufferPool.getAll()) {
//                System.out.println(pageId+ ": " + this.lruBufferPool.get(pageId, false).getPinCount());
                Page page = this.lruBufferPool.get(pageId, false);
                flushList.add(pageId);
                if (page.getIsDirty()) {
                    this.diskManager.writePage(pageId, page.getPageData());
//                    System.out.println("page " + pageId + " is flushed");
                }
            }

            int i;
            for (i = 0; i < flushList.size(); i++) {
//                System.out.println("page " + flushList.get(i) + " is deleted");
                this.lruBufferPool.delete(flushList.get(i));
            }
        }
    }

    /**
     *
     * @param pageId
     * @return
     */
    public boolean deletePage(int pageId) {
        synchronized (this) {
            Page requestPage = this.lruBufferPool.get(pageId, false);
            if (requestPage == null) return false;
            if (requestPage.getPinCount() != 0) return false;

            requestPage.setDirty(false);
            requestPage.setPinCount(0);
            requestPage.resetData();

            this.freeList.addLast(requestPage);
            this.lruBufferPool.delete(pageId);
            this.diskManager.deAllocatePage(pageId);

            return true;
        }
    }

    public void replacePage(Page page) {
        synchronized (this) {
            this.lruBufferPool.replace(page);
        }
    }

    private Page findUnusedPage() {
        synchronized (this) {
            if (!this.freeList.isEmpty()) {
                return this.freeList.getFirst();
            }
            return null;
        }
    }

    // test helper function
    public int getSize() {
        return this.lruBufferPool.getAll().size();
    }
}
