package txDB.buffer;

import txDB.storage.disk.DiskManager;
import txDB.storage.page.Page;
//import myPTA.storage.page.TablePage;

import java.util.HashMap;
//import java.util.concurrent.ConcurrentHashMap;

public class LRUBufferPool {
    private HashMap<Integer, DLinkedNode> bufferPool;
    private DLinkedNode head, tail;
    private int bufferSize;
    private int currentSize;
    private DiskManager diskManager;

    public LRUBufferPool(int bufferSize, DiskManager diskManager) {
        this.diskManager = diskManager;
        this.bufferPool = new HashMap<>(bufferSize);
        this.bufferSize = bufferSize;
        this.currentSize = 0;

        this.head = new DLinkedNode();
        // head.prev = null;

        this.tail = new DLinkedNode();
        // tail.next = null;

        this.head.next = this.tail;
        this.tail.prev = this.head;
    }

    public LRUBufferPool(int bufferSize, float loadFactor, DiskManager diskManager) {
        this.diskManager = diskManager;
        this.bufferPool = new HashMap<>(bufferSize, loadFactor);
        this.bufferSize = bufferSize;
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

//    public boolean allPinned() {
//        if (this.currentSize < this.bufferSize)
//            return false;
//
//        return this.victim() == null;
//    }

    public Page get(int key) {
        DLinkedNode node = this.bufferPool.get(key);
        if (node == null) return null;

        // move the accessed node to the head;
        this.moveToHead(node);

        return node.value;
    }

    public boolean put(int key, Page value) {
        DLinkedNode node = this.bufferPool.get(key);

        if(node == null) {
            /**
             * when there is no evict page, and
             * currentSize will be large than bufferSize,
             * in other words, all pages are currently pinned,
             * new page cannot be put.
             */
            DLinkedNode evictNode = new DLinkedNode();
            if (this.currentSize >= this.bufferSize && (evictNode = this.victim()) == null)
                return false;

            DLinkedNode newNode = new DLinkedNode();
            newNode.key = key;
            newNode.value = value;

            this.bufferPool.put(key, newNode);
            this.addNode(newNode);

            this.currentSize++;

            if (this.currentSize > this.bufferSize) {
                if (evictNode.value.getIsDirty()) {
                    this.diskManager.writePage(evictNode.key, evictNode.value.getPageData());
                }
                this.removeNode(evictNode);
                this.bufferPool.remove(evictNode.key);
                this.currentSize--;
            }
        }
//        else {
//            // update the value.
//            node.value = value;
//            this.moveToHead(node);
//        }

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
}