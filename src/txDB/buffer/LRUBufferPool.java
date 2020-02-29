package txDB.buffer;

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
            // when currentSize < bufferSize, we do not need to do victim()
            if (this.currentSize >= this.bufferSize)
                if ((evictNode = this.victim()) == null)
                    throw new RuntimeException("BUFFER EXCEEDED ERROR");

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
}
