package txDB.storage.index;

import txDB.Config;
import txDB.concurrency.LockManager;
import txDB.concurrency.Transaction.TransactionState;
import txDB.recovery.LogManager;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * This class is an in-memory B+ tree, and
 * it can be a basis of building the true disk-based B+ tree
 *
 * @param <K>
 * @param <V>
 */
public class InMemoryBPlusTreeIndex<K extends Comparable<K>, V> {
    // TODO: not thread safe
    private Node<K, V> root;
    private static int MAXDEGREE;
    private TransactionManager transactionManager;

    public InMemoryBPlusTreeIndex(int maxDegree) {
        MAXDEGREE = maxDegree;
        transactionManager = new TransactionManager();
    }

    public TransactionManager getTransactionManager() {
        return transactionManager;
    }

    public class Transaction {
        private int txnId;
        private TransactionState transactionState;
        private Queue<Node<K, V>> indexNodeQueue;

        public Transaction(int txnId) {
            this.txnId = txnId;
            this.transactionState = TransactionState.GROWING;
            this.indexNodeQueue = new LinkedList<>();
        }

        public int getTxnId() {
            return txnId;
        }

        public TransactionState getTransactionState() {
            return transactionState;
        }

        public void setTransactionState(TransactionState transactionState) {
            this.transactionState = transactionState;
        }

        public Queue<Node<K, V>> getIndexNodeQueue() {
            return indexNodeQueue;
        }

        public void pushIndexNodeQueue(Node<K, V> node) {
            this.indexNodeQueue.add(node);
        }

        public Node<K, V> popIndexNodeQueue() {
            return this.indexNodeQueue.poll();
        }


    }

    public class TransactionManager {
        // TODO
        private HashMap<Integer, Transaction> txnMap;
        private AtomicInteger nextTxnId;

        public TransactionManager() {
            this.txnMap = new HashMap<>();
            this.nextTxnId = new AtomicInteger(0);
        }

        public Transaction begin() {
            Transaction txn = new Transaction(this.nextTxnId.getAndIncrement());
            if (Config.ENABLE_LOGGING) {
                // TODO
            }
            txnMap.put(txn.getTxnId(), txn);
            return txn;
        }

        public void commit(Transaction txn) {
            // TODO
            txn.setTransactionState(TransactionState.COMMITTED);
        }

        public void abort(Transaction txn) {
            // TODO
            txn.setTransactionState(TransactionState.ABORTED);
//            while () {
//
//            }
        }

        public void restart(Transaction txn) {
            // TODO
            txn.setTransactionState(TransactionState.RESTARTED);
//            while () {
//
//            }
        }

        public Transaction getTransaction(int txnId) {
            if (txnMap.containsKey(txnId)) {
                return txnMap.get(txnId);
            }

            return null;
        }
    }

    private class Node<K extends Comparable<K>, V> {
        protected boolean isRootNode;
        protected boolean isLeafNode;
        protected ArrayList<K> keys;
        protected Node<K, V> parent;
        protected ReadWriteLock readWriteLatch;
        protected Lock readLatch;
        protected Lock writeLatch;

        public boolean isOverSized() {
            return keys.size() >= MAXDEGREE;
        }

        public boolean isUnderSized() {
            return keys.size() < Math.round(MAXDEGREE / 2.0 - 1);
        }

        public void setParent(Node<K, V> p) {
            parent = p;
        }

        public Node<K, V> getParent() {
            return parent;
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
    }

    private class InnerNode<K extends Comparable<K>, V> extends Node<K, V> {
        private ArrayList<Node<K, V>> children;

        private InnerNode(K key, Node<K, V> leftNode, Node<K, V> rightNode, boolean isRoot) {
            readWriteLatch = new ReentrantReadWriteLock();
            readLatch = readWriteLatch.readLock();
            writeLatch = readWriteLatch.writeLock();
            isLeafNode = false;
            isRootNode = isRoot;
            keys = new ArrayList<>();
            keys.add(key);
            children = new ArrayList<>();
            children.add(leftNode);
            children.add(rightNode);
        }

        private InnerNode(List<K> ks, List<Node<K, V>> cd, boolean isRoot) {
            readWriteLatch = new ReentrantReadWriteLock();
            readLatch = readWriteLatch.readLock();
            writeLatch = readWriteLatch.writeLock();
            isLeafNode = false;
            isRootNode = isRoot;
            keys = new ArrayList<>(ks);
            children = new ArrayList<>(cd);
        }

        private void setIsRoot(boolean isRoot) {
            isRootNode = isRoot;
        }

        private void insertAndSort(K key, Node<K, V> node) {
            if (key.compareTo(keys.get(0)) < 0) {
                keys.add(0, key);
                // new right node should be at right side where index is key's index plus 1
                children.add(1, node);
            } else if (key.compareTo(keys.get(keys.size() - 1)) > 0) {
                keys.add(key);
                children.add(node);
            } else {
                ListIterator<K> iterator = keys.listIterator();
                while (iterator.hasNext()) {
                    if (iterator.next().compareTo(key) > 0) {
                        int p = iterator.previousIndex();
                        keys.add(p, key);
                        // new right node should be at right side where index is key's index plus 1
                        children.add(p + 1, node);
                        break;
                    }
                }
            }
        }

        private void insertAndSort(K key) {
            if (key.compareTo(keys.get(0)) < 0) {
                keys.add(0, key);
            } else if (key.compareTo(keys.get(keys.size() - 1)) > 0) {
                keys.add(key);
            } else {
                ListIterator<K> iterator = keys.listIterator();
                while (iterator.hasNext()) {
                    if (iterator.next().compareTo(key) > 0) {
                        int p = iterator.previousIndex();
                        keys.add(p, key);
                        break;
                    }
                }
            }
        }

        private void remove(int keyIndex) {
            // TODO: may have bug
            keys.remove(keyIndex);
            children.remove(keyIndex + 1);
        }
    }

    private class LeafNode<K extends Comparable<K>, V> extends Node<K, V> {
        private LeafNode<K, V> prevLeaf;
        private LeafNode<K, V> nextLeaf;
        private ArrayList<V> values;

        private LeafNode(K key, V value) {
            readWriteLatch = new ReentrantReadWriteLock();
            readLatch = readWriteLatch.readLock();
            writeLatch = readWriteLatch.writeLock();
            isLeafNode = true;
            keys = new ArrayList<>();
            values = new ArrayList<>();
            keys.add(key);
            values.add(value);
        }

        private LeafNode(List<K> ks, List<V> vs) {
            readWriteLatch = new ReentrantReadWriteLock();
            readLatch = readWriteLatch.readLock();
            writeLatch = readWriteLatch.writeLock();
            isLeafNode = true;
            keys = new ArrayList<>(ks);
            values = new ArrayList<>(vs);
        }

        private V getValue(K key) {
            ListIterator<K> iterator = keys.listIterator();
            while (iterator.hasNext()) {
                if (iterator.next().compareTo(key) == 0) {
                    return values.get(iterator.previousIndex());
                }
            }
            return null;
        }

        private int getValueIndex(K key) {
            ListIterator<K> iterator = keys.listIterator();
            while (iterator.hasNext()) {
                if (iterator.next().compareTo(key) == 0) {
                    return iterator.previousIndex();
                }
            }
            return 0;
        }

        private void insertAndSort(K key, V value) {
            if (key.compareTo(keys.get(0)) < 0) {
                keys.add(0, key);
                values.add(0, value);
            } else if (key.compareTo(keys.get(keys.size() - 1)) > 0) {
                keys.add(key);
                values.add(value);
            } else {
                ListIterator<K> iterator = keys.listIterator();
                while (iterator.hasNext()) {
                    if (iterator.next().compareTo(key) > 0) {
                        int p = iterator.previousIndex();
                        keys.add(p, key);
                        values.add(p, value);
                        break;
                    }
                }
            }
        }

        private void remove(K key) {
            int p = keys.indexOf(key);
            keys.remove(p);
            values.remove(p);
        }
    }

    /**
     *
     * @param key
     * @param value
     */
    public void insert(K key, V value, Transaction txn) {
        if (root == null) root = new LeafNode<>(key, value);

        insertHelper(root, key, value, txn);
    }

    private void insertHelper(Node<K, V> root, K key, V value, Transaction txn) {
        if (root == null) return;

        if (root.isLeafNode || txn.getTransactionState() == TransactionState.RESTARTED) {
            System.out.println("Inserting " + key.toString() + " in txn " + txn.getTxnId() + ": get write latch");
            root.writeLatch();
        } else {
            System.out.println("Inserting " + key.toString() + " in txn " + txn.getTxnId() + ": get read latch");
            root.readLatch();
        }
        txn.pushIndexNodeQueue(root);
        if (!root.equals(this.root)) {
            Node<K, V> node = txn.popIndexNodeQueue();
            System.out.println("Inserting " + key.toString() + " in txn " + txn.getTxnId() + ": when node with keys: " + root.keys + " get latch, then unlatch node with keys: " + node.keys);
            if (txn.getTransactionState() != TransactionState.RESTARTED) {
                System.out.println("Inserting " + key.toString() + " in txn " + txn.getTxnId() + ": release read latch");
                node.readUnlatch();
            } else {
                System.out.println("Inserting " + key.toString() + " in txn " + txn.getTxnId() + ": release write latch");
                node.writeUnlatch();
            }
        }

        if (root.isLeafNode) {
            if (root.keys.size() + 1 >= MAXDEGREE
                    && txn.getTransactionState() != TransactionState.RESTARTED
                    && !this.root.isLeafNode) {
                Node<K, V> node = txn.popIndexNodeQueue();
                assert node.equals(root);
                assert txn.getIndexNodeQueue().size() == 0;
                System.out.println("Inserting " + key.toString() + " in txn " + txn.getTxnId() + ": release write latch");
                root.writeUnlatch();
                System.out.println("Inserting " + key.toString() + " in txn " + txn.getTxnId() + ": not safe, restart txn");
                transactionManager.restart(txn);
                insertHelper(this.root, key, value, txn);
                txn.setTransactionState(TransactionState.GROWING);
            } else {
                ((LeafNode<K, V>) root).insertAndSort(key, value);
//                System.out.println("leaf node keys: " + root.keys);
                if (root.isOverSized()) {
                    splitLeafNode((LeafNode<K, V>) root);
                }

                Node<K, V> node = txn.popIndexNodeQueue();
                assert node.equals(root);
                assert txn.getIndexNodeQueue().size() == 0;
                System.out.println("Inserting " + key.toString() + " in txn " + txn.getTxnId() + ": release write latch");
                root.writeUnlatch();
            }
        } else {
            if (key.compareTo(root.keys.get(0)) < 0) {
                insertHelper(((InnerNode<K, V>) root).children.get(0), key, value, txn);
            } else if (key.compareTo(root.keys.get(root.keys.size() - 1)) >= 0) {
                insertHelper(((InnerNode<K, V>) root).children.get(((InnerNode<K, V>) root).children.size() - 1), key, value, txn);
            } else {
                int i;
                for (i = 1; i < root.keys.size(); i++) {
                    if (((InnerNode<K, V>) root).keys.get(i).compareTo(key) > 0) {
                        insertHelper(((InnerNode<K, V>) root).children.get(i), key, value, txn);
                        break;
                    }
                }
            }
        }
    }

    /**
     *
     * @param leafNode
     */
    private void splitLeafNode(LeafNode<K, V> leafNode) {
        int from = MAXDEGREE / 2, to = leafNode.keys.size();
        K splitKey = leafNode.keys.get(from);

        LeafNode<K, V> rightNode = new LeafNode<>(
                leafNode.keys.subList(from, to),
                leafNode.values.subList(from, to)
        );
        leafNode.keys.subList(from, to).clear();
        leafNode.values.subList(from, to).clear();

        if (leafNode.nextLeaf != null) {
            leafNode.nextLeaf.prevLeaf = rightNode;
            rightNode.nextLeaf = leafNode.nextLeaf;
        }
        rightNode.prevLeaf = leafNode;
        leafNode.nextLeaf = rightNode;

//        System.out.println("right leaf node keys: " + rightNode.keys);

        if (root.isLeafNode) {
            // it is the first time of splitting
            root = new InnerNode<>(splitKey, leafNode, rightNode, true);
            leafNode.setParent(root);
            rightNode.setParent(root);
        } else {
            Node<K, V> parentNode = leafNode.getParent();
            rightNode.setParent(parentNode);
            ((InnerNode<K, V>) parentNode).insertAndSort(splitKey, rightNode);
//            System.out.println("parent node keys: " + parentNode.keys);
//            for (int i = 0; i < ((InnerNode<K, V>) parentNode).children.size(); i++)
//                System.out.println(((InnerNode<K, V>) parentNode).children.get(i).keys);
            if (parentNode.isOverSized()) {
//                System.out.println("enter inner node split");
                splitInnerNode(((InnerNode<K, V>) parentNode));
            }
        }
    }

    /**
     *
     * @param innerNode
     */
    private void splitInnerNode(InnerNode<K, V> innerNode) {
//        System.out.println("enter inner node split");
        int from = MAXDEGREE / 2, keysTo = innerNode.keys.size(), childrenTo = innerNode.children.size();
        K splitKey = innerNode.keys.get(from);

        InnerNode<K, V> rightIntenalNode = new InnerNode<K, V>(
                innerNode.keys.subList(from + 1, keysTo),
                innerNode.children.subList(from + 1, childrenTo),
                false
        );
        innerNode.keys.subList(from, keysTo).clear();
        innerNode.children.subList(from + 1, childrenTo).clear();

//        System.out.println(splitKey + ", " + innerNode.keys + ", " + rightIntenalNode.keys);

        // assign left and right inner nodes to different parents
        int i;
        for (i = 0; i < innerNode.children.size(); i++) {
            innerNode.children.get(i).setParent(innerNode);
        }

        for (i = 0; i < rightIntenalNode.children.size(); i++) {
            rightIntenalNode.children.get(i).setParent(rightIntenalNode);
        }

        if (innerNode.isRootNode) {
//            System.out.println("inner node is root node");
            innerNode.setIsRoot(false);
            root = new InnerNode<>(splitKey, innerNode, rightIntenalNode, true);
            innerNode.setParent(root);
            rightIntenalNode.setParent(root);
//            for (int i = 0; i < innerNode.children.size(); i++)
//                System.out.println(innerNode.children.get(i).keys);
//            for (int i = 0; i < rightIntenalNode.children.size(); i++)
//                System.out.println(rightIntenalNode.children.get(i).keys);
        } else {
//            System.out.println("inner node is not root node");
            Node<K, V> parentNode = innerNode.getParent();
            ((InnerNode<K, V>) parentNode).insertAndSort(splitKey, rightIntenalNode);
            rightIntenalNode.setParent(parentNode);
//            System.out.println(parentNode.keys);
            if (parentNode.isOverSized()) {
                splitInnerNode(((InnerNode<K, V>) parentNode));
            }
        }
    }

    /**
     *
     * @param key
     * @return
     */
    public V find(K key, Transaction txn) {
        LeafNode<K, V> targetLeafNode = findHelper(root, key, txn);
        return targetLeafNode == null ? null : targetLeafNode.getValue(key);
    }

    private LeafNode<K, V> findHelper(Node<K, V> root, K key, Transaction txn) {
        if (root == null) return null;

        root.readLatch();
        txn.pushIndexNodeQueue(root);
        if (!root.equals(this.root)) {
            Node<K, V> node = txn.popIndexNodeQueue();
            System.out.println("Finding " + key.toString() + " in txn " + txn.getTxnId() + ": when node with keys: " + root.keys + " get latch, then unlatch node with keys: " + node.keys);
            node.readUnlatch();
        }

        if (root.isLeafNode) {
            System.out.println("Finding " + key.toString() + " in txn " + txn.getTxnId() + ": finally, unlatch node with keys: " + root.keys);
            Node<K, V> node = txn.popIndexNodeQueue();
            assert node.equals(root);
            root.readUnlatch();
            return ((LeafNode<K, V>) root);
        } else {
            if (key.compareTo(root.keys.get(0)) < 0) {
                return findHelper(((InnerNode<K, V>) root).children.get(0), key, txn);
            } else if (key.compareTo(root.keys.get(root.keys.size() - 1)) >= 0) {
                return findHelper(((InnerNode<K, V>) root).children.get(((InnerNode<K, V>) root).children.size() - 1), key, txn);
            } else {
                int i;
                for (i = 1; i < root.keys.size(); i++) {
                    if (((InnerNode<K, V>) root).keys.get(i).compareTo(key) > 0)
                        return findHelper(((InnerNode<K, V>) root).children.get(i), key, txn);
                }
            }
        }

        return null;
    }

    /**
     *
     * @param key
     */
    public void delete(K key) {
        if (root != null) {
            deleteHelper(root, key, -1);
        }
    }

    /**
     *
     * @param root
     * @param key
     * @param keyIndex
     */
    private void deleteHelper(Node<K, V> root, K key, int keyIndex) {
        if (root.isLeafNode) {
//            System.out.println("key: " + key);
            ((LeafNode<K, V>) root).remove(key);
            // -1 means root is both rootNode and leafNode, so we do not do redistribution
            if (root.isUnderSized() && keyIndex != -1) {
//                System.out.println("undersized");
                redistributeLeafNode((LeafNode<K, V>) root, keyIndex);
            }
        } else {
            if (key.compareTo(root.keys.get(0)) < 0) {
                deleteHelper(((InnerNode<K, V>) root).children.get(0), key, 0);
            } else if (key.compareTo(root.keys.get(root.keys.size() - 1)) >= 0) {
                deleteHelper(((InnerNode<K, V>) root).children.get(((InnerNode<K, V>) root).children.size() - 1), key, ((InnerNode<K, V>) root).children.size() - 1);
            } else {
                int i;
                for (i = 1; i < root.keys.size(); i++) {
                    if (((InnerNode<K, V>) root).keys.get(i).compareTo(key) > 0) {
                        deleteHelper(((InnerNode<K, V>) root).children.get(i), key, i);
                        break;
                    }
                }
            }
        }
    }

    /**
     *
     * @param smallNode
     * @param largeNode
     * @param keyIndex
     */
    private void mergeLeafNode(LeafNode<K, V> smallNode, LeafNode<K, V> largeNode, int keyIndex) {
        smallNode.keys.addAll(largeNode.keys);
        smallNode.values.addAll(largeNode.values);

        smallNode.nextLeaf = largeNode.nextLeaf;
        if (largeNode.nextLeaf != null) {
            largeNode.nextLeaf.prevLeaf = smallNode;
        }

        InnerNode<K, V> parent = (InnerNode<K, V>) smallNode.getParent();
        parent.remove(keyIndex);
//        System.out.println("Leaf parent keys: " + parent.keys);

        if (!parent.isRootNode && parent.isUnderSized()) {
            redistributeInnerNode(parent);
        }

        if (parent.isRootNode && parent.keys.size() == 0) {
            root = smallNode;
        }
    }

    /**
     *
     * @param smallNode
     * @param largeNode
     */
    private void mergeInnerNode(InnerNode<K, V> smallNode, InnerNode<K, V> largeNode, int parentIndex) {
//        System.out.println("inner parentIndex: " + parentIndex);
        InnerNode<K, V> parent = (InnerNode<K, V>) smallNode.getParent();

        smallNode.keys.add(parent.keys.get(parentIndex));
        smallNode.keys.addAll(largeNode.keys);
        smallNode.children.addAll(largeNode.children);

        parent.remove(parentIndex);

//        System.out.println("inner parent keys: " + parent.keys);

        if (!parent.isRootNode && parent.isUnderSized()) {
            redistributeInnerNode(parent);
        }

        if (parent.isRootNode && parent.keys.size() == 0) {
            root = smallNode;
            smallNode.setIsRoot(true);
        }
    }

    /**
     *
     * @param leafNode
     * @param keyIndex
     */
    private void redistributeLeafNode(LeafNode<K, V> leafNode, int keyIndex) {
        // leaf node is at the leftmost when keyIndex == 0
        if (keyIndex == 0) {
            redistributeLeafNodeHelper(leafNode, leafNode.nextLeaf, keyIndex + 1);
        } else {
            redistributeLeafNodeHelper(leafNode.prevLeaf, leafNode, keyIndex);
        }
    }

    /**
     *
     * @param smallNode
     * @param largeNode
     * @param keyIndex means index of large index node in its parent's children list (largeNode)
     */
    private void redistributeLeafNodeHelper(LeafNode<K, V> smallNode, LeafNode<K, V> largeNode, int keyIndex) {
        int totalSize = smallNode.keys.size() + largeNode.keys.size();
//        System.out.println("keyIndex: " + keyIndex);
//        System.out.println("totalSize: "+ totalSize);
        if (totalSize >= 2 * Math.round(MAXDEGREE / 2.0 - 1)) {
            /**
             * Reference: https://github.com/tiejian/database-hw2/blob/master/BPlusTree.java
             */
            // Store all keys and values from left to right
            ArrayList<K> keys = new ArrayList<>();
            ArrayList<V> vals = new ArrayList <>();
            keys.addAll(smallNode.keys);
            keys.addAll(largeNode.keys);
            vals.addAll(smallNode.values);
            vals.addAll(largeNode.values);

            int leftSize = totalSize / 2;

            smallNode.keys.clear();
            largeNode.keys.clear();
            smallNode.values.clear();
            largeNode.values.clear();

            // Add first half keys and values into left and rest into right
            smallNode.keys.addAll(keys.subList(0, leftSize));
            smallNode.values.addAll(vals.subList(0, leftSize));
            largeNode.keys.addAll(keys.subList(leftSize, keys.size()));
            largeNode.values.addAll(vals.subList(leftSize, vals.size()));

            smallNode.getParent().keys.set(keyIndex - 1, largeNode.keys.get(0));
        } else {
            mergeLeafNode(smallNode, largeNode, keyIndex - 1);
        }
    }

    /**
     *
     * @param parent
     */
    private void redistributeInnerNode(InnerNode<K, V> parent) {
        int i, parentIndex = -1;
        InnerNode<K, V> grandParent = (InnerNode<K, V>) parent.getParent();

        // find index of parent in grandparent's children list
        for (i = 0; i < grandParent.children.size(); i++) {
            if (grandParent.children.get(i) == parent) {
                parentIndex = i;
                break;
            }
        }

//        System.out.println("parentIndex: " + parentIndex);

        InnerNode<K, V> sibling;
        if (parentIndex == 0) {
            sibling = (InnerNode<K, V>) grandParent.children.get(parentIndex + 1);
            redistributeInnerNodeHelper(parent, sibling, parentIndex);
        } else {
            sibling = (InnerNode<K, V>) grandParent.children.get(parentIndex - 1);
            redistributeInnerNodeHelper(sibling, parent, parentIndex - 1);
        }
    }

    /**
     *
     * @param smallNode
     * @param largeNode
     * @param parentIndex
     */
    private void redistributeInnerNodeHelper(InnerNode<K, V> smallNode, InnerNode<K, V> largeNode, int parentIndex) {
        int totalSize = smallNode.keys.size() + largeNode.keys.size();
//        System.out.println("inner totalSize: " + totalSize);
        if (totalSize >= 2 * Math.round(MAXDEGREE / 2.0 - 1)) {
            /**
             * Reference: https://github.com/tiejian/database-hw2/blob/master/BPlusTree.java
             */
            // Store all keys and values from left to right
            ArrayList<K> keys = new ArrayList<>();
            ArrayList<Node<K, V>> kids = new ArrayList<>();
            keys.addAll(smallNode.keys);
            keys.add(smallNode.getParent().keys.get(parentIndex));
            keys.addAll(largeNode.keys);
            kids.addAll(smallNode.children);
            kids.addAll(largeNode.children);

            // Get the index of the new parent key
            int newIndex = keys.size() / 2;
            if (keys.size() % 2 == 0) {
                newIndex -= 1;
            }
            smallNode.getParent().keys.set(parentIndex, keys.get(newIndex));

            smallNode.keys.clear();
            largeNode.keys.clear();
            smallNode.children.clear();
            largeNode.children.clear();

            // Add first half keys and values into left and rest into right
            smallNode.keys.addAll(keys.subList(0, newIndex));
            largeNode.keys.addAll(keys.subList(newIndex + 1, keys.size()));
            smallNode.children.addAll(kids.subList(0, newIndex + 1));
            largeNode.children.addAll(kids.subList(newIndex + 1, kids.size()));
        } else {
            mergeInnerNode(smallNode, largeNode, parentIndex);
        }
    }

    /**
     * traverse the whole B+ tree using BFS
     */
    public void traverseAllNodes() {
        Queue<Node<K, V>> queue = new LinkedList<>();
        if (root != null) {
            queue.add(root);
            while (!queue.isEmpty()) {
                Node<K, V> curNode = queue.poll();

                System.out.println(curNode.keys + ", ");

                if (!curNode.isLeafNode) {
                    int i;
                    for (i = 0; i < ((InnerNode<K, V>) curNode).children.size(); i++) {
                        queue.add(((InnerNode<K, V>) curNode).children.get(i));
                    }
                }
            }
        }
    }

    /**
     * traverse only leaf nodes in B+ tree
     */
    public void traverseLeafNodes() {
        LeafNode<K, V> curLeafNode = traverseLeafNodesHelper(root);
//        LeafNode<K, V> curLeafNode = findHelper(root, key);

        while (curLeafNode != null) {
            System.out.println(curLeafNode.keys + ", " + curLeafNode.values);
            curLeafNode = curLeafNode.nextLeaf;
//            curLeafNode = curLeafNode.prevLeaf;
        }
    }

    /**
     * return the first leaf node using leftmost DFS
     * @param root
     * @return
     */
    private LeafNode<K, V> traverseLeafNodesHelper(Node<K, V> root) {
        if (root == null) return null;
        else if (root.isLeafNode) return ((LeafNode<K, V>) root);
        else {
            return traverseLeafNodesHelper(((InnerNode<K, V>) root).children.get(0));
        }
    }

    /**
     * range scanning through leaf nodes, e.g., select * from table0 where col0 > 50;
     * @param key
     * @param isLarger
     * @return
     */
    public List<V> scanLeafNode(K key, boolean isLarger, Transaction txn) {
        ArrayList<V> res = new ArrayList<>();
        LeafNode<K, V> curLeafNode = findHelper(root, key, txn);

        if (curLeafNode != null) {
            int startIndex = curLeafNode.getValueIndex(key);
            if (isLarger) {
                res.addAll(curLeafNode.values.subList(startIndex, curLeafNode.values.size()));
                curLeafNode = curLeafNode.nextLeaf;
                while (curLeafNode != null) {
                    res.addAll(curLeafNode.values);
                    curLeafNode = curLeafNode.nextLeaf;
                }
            } else {
                ArrayList<V> tmp = new ArrayList<>(curLeafNode.values.subList(startIndex, curLeafNode.values.size()));
                Collections.reverse(tmp);
                res.addAll(tmp);
                curLeafNode = curLeafNode.prevLeaf;
                while (curLeafNode != null) {
                    tmp = new ArrayList<>(curLeafNode.values);
                    Collections.reverse(tmp);
                    res.addAll(tmp);
                    curLeafNode = curLeafNode.prevLeaf;
                }
            }
        }

        return res;
    }
}
