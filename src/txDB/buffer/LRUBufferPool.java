package txDB.buffer;

import txDB.Config;
import txDB.recovery.LogManager;
import txDB.storage.disk.DiskManager;
import txDB.storage.page.Page;
//import txDB.storage.page.TablePage;

import java.util.HashMap;
import java.util.Set;
//import java.util.concurrent.ConcurrentHashMap;

public class LRUBufferPool {
    private HashMap<Integer, DLinkedNode> bufferPool;
    private DLinkedNode head, tail;
    private int bufferSize;
    private int currentSize;
    private DiskManager diskManager;
    private LogManager logManager;

    public LRUBufferPool(int bufferSize, DiskManager diskManager, LogManager logManager) {
        this.diskManager = diskManager;
        this.logManager = logManager;
        this.bufferPool = new HashMap<>(Config.BUFFER_SIZE);
        this.bufferSize = Config.BUFFER_SIZE;
        this.currentSize = 0;

        this.head = new DLinkedNode();
        // head.prev = null;

        this.tail = new DLinkedNode();
        // tail.next = null;

        this.head.next = this.tail;
        this.tail.prev = this.head;
    }

    public LRUBufferPool(int bufferSize, float loadFactor, DiskManager diskManager, LogManager logManager) {
        this.diskManager = diskManager;
        this.logManager = logManager;
        this.bufferPool = new HashMap<>(Config.BUFFER_SIZE, loadFactor);
        this.bufferSize = Config.BUFFER_SIZE;
        this.currentSize = 0;

        this.head = new DLinkedNode();
        // head.prev = null;

        this.tail = new DLinkedNode();
        // tail.next = null;

        this.head.next = this.tail;
        this.tail.prev = this.head;
    }

    private class DLinkedNode {
        int key;
        Page value;
        DLinkedNode prev;
        DLinkedNode next;
    }

    private void addNode(DLinkedNode node) {
        /**
         * Always add the new node right after head.
         */
        node.prev = head;
        node.next = head.next;

        head.next.prev = node;
        head.next = node;
    }

    private void removeNode(DLinkedNode node) {
        /**
         * Remove an existing node from the linked list.
         */
        DLinkedNode prev = node.prev;
        DLinkedNode next = node.next;

        prev.next = next;
        next.prev = prev;
    }

    private void moveToHead(DLinkedNode node) {
        /**
         * Move certain node in between to the head.
         */
        this.removeNode(node);
        this.addNode(node);
    }

//    private DLinkedNode popTail() {
//        /**
//         * Pop the current tail.
//         */
//        DLinkedNode res = this.tail.prev;
//        this.removeNode(res);
//        return res;
//    }

    // TODO: linear searching to find a victim page may be a bottleneck in the future !!!
    private DLinkedNode victim() {
        DLinkedNode cur = this.tail.prev;
        DLinkedNode res = null;
        while (cur != this.head) {
            if (cur.value.getPinCount() == 0) {
                res = cur;
                break;
            }
            cur = cur.prev;
        }
        return res;
    }

//    public Page victim() {
//        DLinkedNode evictNode = this.victimHelper();
//        if (this.currentSize >= this.bufferSize && evictNode == null)
//            return null;
//        else return evictNode.value;
//    }

//    public boolean allPinned() {
//        if (this.currentSize < this.bufferSize)
//            return false;
//
//        return this.victim() == null;
//    }

    public Page get(int key, boolean applyLRU) {
        DLinkedNode node = this.bufferPool.get(key);
        if (node == null) return null;

        // if applyLRU is true, move the accessed node to the head;
        if (applyLRU) this.moveToHead(node);

        return node.value;
    }

    public boolean put(int key, Page value) {
        DLinkedNode node = this.bufferPool.get(key);

        if(node == null) {
            /**
             * when there is no evict page, and
             * currentSize will be large than bufferSize,
             * in other words, when all pages are currently pinned,
             * new page cannot be put.
             */
            DLinkedNode evictNode = new DLinkedNode();
            // when currentSize < bufferSize, we do not need to do victim() for efficiency issues
            if (this.currentSize >= this.bufferSize)
                if ((evictNode = this.victim()) == null)
                    // we do not throw RuntimeException here to keep system going
                    throw new RuntimeException("BUFFER EXCEEDED ERROR");
//                    return false;

            DLinkedNode newNode = new DLinkedNode();
            newNode.key = key;
            newNode.value = value;

            this.bufferPool.put(key, newNode);
            this.addNode(newNode);

            this.currentSize++;

            if (this.currentSize > this.bufferSize) {
//                System.out.println("currentSize > bufferSize");
//                System.out.println(evictNode.value.getPageId() + ": " + evictNode.value.getIsDirty());
                if (evictNode.value.getIsDirty()) {
//                    System.out.println("page " + evictNode.key + " is flushed");
                    if (Config.ENABLE_LOGGING && logManager.getFlushedLsn() < evictNode.value.getLsn()) {
                        System.out.println("buffer manager wait for log flush when evicting pages");
                        logManager.flushLogBuffer(true, false);
                    }
                    assert logManager.getFlushedLsn() >= evictNode.value.getLsn();
                    this.diskManager.writePage(evictNode.key, evictNode.value.getPageData());
                }
                this.removeNode(evictNode);
                this.bufferPool.remove(evictNode.key);
                this.currentSize--;
            }
        }

        return true;
    }

    public void delete(int key) {
        DLinkedNode node = this.bufferPool.get(key);

        if (node != null) {
            this.removeNode(node);
            this.bufferPool.remove(node.key);
            this.currentSize--;
        }
    }

    public Set<Integer> getAll() {
        return this.bufferPool.keySet();
    }

    public void replace(Page page) {
        DLinkedNode node = this.bufferPool.get(page.getPageId());
        node.value = page;
    }
}
