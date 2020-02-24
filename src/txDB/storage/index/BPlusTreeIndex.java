package txDB.storage.index;

import txDB.buffer.BufferManager;
import txDB.Config;
import txDB.storage.page.*;

import java.io.*;

/**
 * This class is a disk-based B+ tree
 */
public class BPlusTreeIndex<K extends Comparable<K>, V> {
    // TODO
    private BufferManager bufferManager;
    private int rootPageId;
    private BPlusTreePageNode<K, V> rootPageNode;
    private static int MAXDEGREE;

    @SuppressWarnings("unchecked")
    public BPlusTreeIndex(BufferManager bufferManager, int rootPageId, int maxDegree) {
        this.bufferManager = bufferManager;
        this.rootPageId = rootPageId;
        MAXDEGREE = maxDegree;
        if (!isEmpty()) {
            Page rootPage = bufferManager.fetchPage(rootPageId);
            try {
                ByteArrayInputStream bis = new ByteArrayInputStream(rootPage.getPageData());
                ObjectInputStream in = new ObjectInputStream(bis);
                rootPageNode = (BPlusTreePageNode) in.readObject();
            } catch (IOException | ClassNotFoundException e) {
                e.printStackTrace();
            }
        }
    }

    /**
     *
     * @return
     */
    private boolean isEmpty() {
        return rootPageId == Config.INVALID_PAGE_ID;
    }

    /**
     *
     * @param key
     * @param value
     */
    public void insert(K key, V value) {
        if (isEmpty()) {
            System.out.println("isEmpty");
            startNewTree(key, value);
            return;
        }
        System.out.println("rootPageId: " + rootPageId);
        insertHelper(rootPageId, key, value);
    }

    /**
     *
     * @param key
     * @param value
     */
    private void startNewTree(K key, V value) {
        Page rootPage = bufferManager.newPage();
        rootPageNode = new BPlusTreeLeafPageNode<>(key, value, rootPage.getPageId(), Config.INVALID_PAGE_ID, MAXDEGREE);
        rootPageId = rootPage.getPageId();
        ((BPlusTreeLeafPageNode<K, V>) rootPageNode).insertAndSort(key, value);

        try {
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            ObjectOutput out = new ObjectOutputStream(bos);
            out.writeObject(rootPageNode);
            rootPage.setPageData(bos.toByteArray());
            bufferManager.unpinPage(rootPageId, true);
            bufferManager.flushPage(rootPageId);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     *
     * @param rootPageId
     * @param key
     * @param value
     */
    @SuppressWarnings("unchecked")
    private void insertHelper(int rootPageId, K key, V value) {
        Page rootPage = bufferManager.fetchPage(rootPageId);
        try {
            ByteArrayInputStream bis = new ByteArrayInputStream(rootPage.getPageData());
            ObjectInputStream in = new ObjectInputStream(bis);
            BPlusTreePageNode<K, V> bPlusTreePageNode = (BPlusTreePageNode<K, V>) in.readObject();

            if (bPlusTreePageNode.isLeafPageNode()) {
                System.out.println("isLeafPageNode and maxSize: " + bPlusTreePageNode.getMaxSize());
                ((BPlusTreeLeafPageNode<K, V>) bPlusTreePageNode).insertAndSort(key, value);

                ByteArrayOutputStream bos = new ByteArrayOutputStream();
                ObjectOutput out = new ObjectOutputStream(bos);
                out.writeObject(bPlusTreePageNode);
                rootPage.setPageData(bos.toByteArray());

                System.out.println(bPlusTreePageNode.getKeys().size());
                if (bPlusTreePageNode.isOverSized()) {
                    System.out.println("isOversized");
                    splitLeafNode((BPlusTreeLeafPageNode<K, V>) bPlusTreePageNode);
                }
            } else {
                if (key.compareTo(bPlusTreePageNode.getKeys().get(0)) < 0) {
                    insertHelper(((BPlusTreeInnerPageNode<K, V>) bPlusTreePageNode).getChildren().get(0), key, value);
                } else if (key.compareTo(bPlusTreePageNode.getKeys().get(bPlusTreePageNode.getKeys().size() - 1)) >= 0) {
                    insertHelper(((BPlusTreeInnerPageNode<K, V>) bPlusTreePageNode).getChildren().get(((BPlusTreeInnerPageNode<K, V>) bPlusTreePageNode).getChildren().size() - 1), key, value);
                } else {
                    // TODO: use Binary Search instead of linear search
                    int i;
                    for (i = 1; i < bPlusTreePageNode.getKeys().size(); i++) {
                        if (bPlusTreePageNode.getKeys().get(i).compareTo(key) > 0)
                            insertHelper(((BPlusTreeInnerPageNode<K, V>) bPlusTreePageNode).getChildren().get(i), key, value);
                    }
                }
            }
        } catch (IOException | ClassNotFoundException e) {
            e.printStackTrace();
        }
    }

    @SuppressWarnings("unchecked")
    private void splitLeafNode(BPlusTreeLeafPageNode<K, V> leftLeafPageNode) {
        int from = MAXDEGREE / 2, to = leftLeafPageNode.getKeys().size();
        K splitKey = leftLeafPageNode.getKeys().get(from);

        Page newPage = bufferManager.newPage();
        BPlusTreeLeafPageNode<K, V> rightLeafPageNode = new BPlusTreeLeafPageNode<>(
                leftLeafPageNode.getKeys().subList(from, to),
                leftLeafPageNode.getValues().subList(from, to),
                newPage.getPageId(),
                leftLeafPageNode.getParentPageId(),
                MAXDEGREE
        );
        leftLeafPageNode.getKeys().subList(from, to).clear();
        leftLeafPageNode.getValues().subList(from, to).clear();

        ByteArrayInputStream bis;
        ObjectInputStream in;

        if (leftLeafPageNode.getNextPageId() != Config.INVALID_PAGE_ID) {
            Page nextPage = bufferManager.fetchPage(leftLeafPageNode.getNextPageId());
            try {
                bis = new ByteArrayInputStream(nextPage.getPageData());
                in = new ObjectInputStream(bis);
                BPlusTreeLeafPageNode<K, V> nextLeafPageNode = (BPlusTreeLeafPageNode<K, V>) in.readObject();
                nextLeafPageNode.setPrevPageId(rightLeafPageNode.getPageId());
            } catch (IOException | ClassNotFoundException e) {
                e.printStackTrace();
            }
            rightLeafPageNode.setNextPageId(leftLeafPageNode.getNextPageId());
        }
        rightLeafPageNode.setPrevPageId(leftLeafPageNode.getPageId());
        leftLeafPageNode.setNextPageId(rightLeafPageNode.getPageId());

        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        ObjectOutput out;

        if (rootPageNode.isLeafPageNode()) {
            System.out.println("root page node is leaf node");
            // it is the first time of splitting
            Page leftPage = bufferManager.newPage();
            leftLeafPageNode.setPageId(leftPage.getPageId());
            leftLeafPageNode.setParentPageId(rootPageId);
            rightLeafPageNode.setParentPageId(rootPageId);
            rootPageNode = new BPlusTreeInnerPageNode<>(splitKey, leftLeafPageNode.getPageId(), rightLeafPageNode.getPageId(), rootPageNode.getPageId(), rootPageNode.getParentPageId(), MAXDEGREE);
            System.out.println("root node type: " + rootPageNode.getIndexPageType());
            try {
                out = new ObjectOutputStream(bos);

                out.writeObject(rightLeafPageNode);
                newPage.setPageData(bos.toByteArray());

                bos = new ByteArrayOutputStream();
                out = new ObjectOutputStream(bos);
                out.writeObject(leftLeafPageNode);
                leftPage.setPageData(bos.toByteArray());

                bos = new ByteArrayOutputStream();
                out = new ObjectOutputStream(bos);
                out.writeObject(rootPageNode);
                Page rootPage = bufferManager.fetchPage(rootPageNode.getPageId());
                rootPage.setPageData(bos.toByteArray());

                bufferManager.unpinPage(newPage.getPageId(), true);
                bufferManager.flushPage(newPage.getPageId());
                bufferManager.unpinPage(leftPage.getPageId(), true);
                bufferManager.flushPage(leftPage.getPageId());
                bufferManager.unpinPage(rootPage.getPageId(), true);
                bufferManager.flushPage(rootPage.getPageId());
            } catch (IOException e) {
                e.printStackTrace();
            }
        } else {
            Page parentPage = bufferManager.fetchPage(leftLeafPageNode.getParentPageId());
            try {
                bis = new ByteArrayInputStream(parentPage.getPageData());
                in = new ObjectInputStream(bis);
                BPlusTreePageNode<K, V> parentPageNode = (BPlusTreePageNode<K, V>) in.readObject();
                ((BPlusTreeInnerPageNode<K, V>) parentPageNode).insertAndSort(splitKey, rightLeafPageNode.getPageId());

                rightLeafPageNode.setParentPageId(leftLeafPageNode.getParentPageId());
                out = new ObjectOutputStream(bos);
                out.writeObject(rightLeafPageNode);
                newPage.setPageData(bos.toByteArray());

                out.writeObject(leftLeafPageNode);
                Page leftPage = bufferManager.fetchPage(leftLeafPageNode.getPageId());
                leftPage.setPageData(bos.toByteArray());

                bufferManager.unpinPage(newPage.getPageId(), true);
                bufferManager.flushPage(newPage.getPageId());
                bufferManager.unpinPage(leftPage.getPageId(), true);
                bufferManager.flushPage(leftPage.getPageId());

                if (parentPageNode.isOverSized()) {
                    splitInnerNode((BPlusTreeInnerPageNode<K, V>) parentPageNode);
                }
            } catch (IOException | ClassNotFoundException e) {
                e.printStackTrace();
            }
        }
    }

    private void splitInnerNode(BPlusTreeInnerPageNode<K, V> innerPageNode) {

    }

    public V find(K key) {
        BPlusTreeLeafPageNode<K, V> targetPageNode = findHelper(rootPageId, key);
        return targetPageNode == null ? null : targetPageNode.getValue(key);
    }

    @SuppressWarnings("unchecked")
    private BPlusTreeLeafPageNode<K, V> findHelper(int rootPageId, K key) {
        try {
            Page rootPage = bufferManager.fetchPage(rootPageId);
            ByteArrayInputStream bis = new ByteArrayInputStream(rootPage.getPageData());
            ObjectInputStream in = new ObjectInputStream(bis);
            BPlusTreePageNode<K, V> root = (BPlusTreePageNode<K, V>) in.readObject();

            if (root.getPageId() == Config.INVALID_PAGE_ID) return null;
            else if (root.isLeafPageNode()) return ((BPlusTreeLeafPageNode<K, V>) root);
            else {
                if (key.compareTo(root.getKeys().get(0)) < 0) {
                    return findHelper(((BPlusTreeInnerPageNode<K, V>) root).getChildren().get(0), key);
                } else if (key.compareTo(root.getKeys().get(root.getKeys().size() - 1)) >= 0) {
                    return findHelper(((BPlusTreeInnerPageNode<K, V>) root).getChildren().get(((BPlusTreeInnerPageNode<K, V>) root).getChildren().size() - 1), key);
                } else {
                    // TODO: use Binary Search instead of linear search
                    int i;
                    for (i = 1; i < root.getKeys().size(); i++) {
                        if (root.getKeys().get(i).compareTo(key) > 0)
                            return findHelper(((BPlusTreeInnerPageNode<K, V>) root).getChildren().get(i), key);
                    }
                }
            }
        } catch (IOException | ClassNotFoundException e) {
            e.printStackTrace();
        }

        return null;
    }
}
