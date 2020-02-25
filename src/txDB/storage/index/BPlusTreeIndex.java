package txDB.storage.index;

import txDB.buffer.BufferManager;
import txDB.Config;
import txDB.storage.page.*;

import java.io.*;
import java.util.*;

/**
 * This class is a disk-based B+ tree
 */
public class BPlusTreeIndex<K extends Comparable<K>, V> {
    // TODO: some code need to be refactored !!!
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
//            System.out.println("isEmpty");
            startNewTree(key, value);
            return;
        }
//        System.out.println("rootPageId: " + rootPageId);
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
                ((BPlusTreeLeafPageNode<K, V>) bPlusTreePageNode).insertAndSort(key, value);

                ByteArrayOutputStream bos = new ByteArrayOutputStream();
                ObjectOutput out = new ObjectOutputStream(bos);
                out.writeObject(bPlusTreePageNode);
                rootPage.setPageData(bos.toByteArray());

                if (bPlusTreePageNode.isOverSized()) {
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
//        System.out.println("leafNode isOversized");
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

        ByteArrayInputStream bis = null;
        ObjectInputStream in = null;

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
        ObjectOutput out = null;

        if (rootPageNode.isLeafPageNode()) {
//            System.out.println("root page node is leaf node");
            // it is the first time of splitting
            Page leftPage = bufferManager.newPage();
            leftLeafPageNode.setPageId(leftPage.getPageId());
            leftLeafPageNode.setParentPageId(rootPageId);
            rightLeafPageNode.setParentPageId(rootPageId);
            rootPageNode = new BPlusTreeInnerPageNode<>(splitKey, leftLeafPageNode.getPageId(), rightLeafPageNode.getPageId(), rootPageNode.getPageId(), rootPageNode.getParentPageId(), MAXDEGREE);
            handleRootPageNode(newPage, leftPage, rightLeafPageNode, leftLeafPageNode, bos, out);
        } else {
            BPlusTreePageNode<K, V> parentPageNode = handleParentPageNode(splitKey, leftLeafPageNode, rightLeafPageNode, bis, in, bos, out);

            serializePageNode(newPage, bos, out, rightLeafPageNode);

            Page leftPage = bufferManager.fetchPage(leftLeafPageNode.getPageId());
            serializePageNode(leftPage, bos, out, leftLeafPageNode);

            if (parentPageNode.isOverSized()) {
                splitInnerNode((BPlusTreeInnerPageNode<K, V>) parentPageNode);
            }
        }
    }

    @SuppressWarnings("unchecked")
    private void splitInnerNode(BPlusTreeInnerPageNode<K, V> leftInnerPageNode) {
//        System.out.println("innerNode isOverSized");
        int from = MAXDEGREE / 2, keysTo = leftInnerPageNode.getKeys().size(), childrenTo = leftInnerPageNode.getChildren().size();
        K splitKey = leftInnerPageNode.getKeys().get(from);

        Page newPage = bufferManager.newPage();
        BPlusTreeInnerPageNode<K, V> rightInnerPageNode = new BPlusTreeInnerPageNode<>(
                leftInnerPageNode.getKeys().subList(from + 1, keysTo),
                leftInnerPageNode.getChildren().subList(from + 1, childrenTo),
                newPage.getPageId(),
                leftInnerPageNode.getParentPageId(),
                MAXDEGREE
        );
        leftInnerPageNode.getKeys().subList(from, keysTo).clear();
        leftInnerPageNode.getChildren().subList(from + 1, childrenTo).clear();

        Page leftPage;
        if (leftInnerPageNode.isRootPageNode()) {
            leftPage = bufferManager.newPage();
            leftInnerPageNode.setPageId(leftPage.getPageId());
        } else {
            leftPage = bufferManager.fetchPage(leftInnerPageNode.getPageId());
        }

//        System.out.println(splitKey + ", " + leftInnerPageNode.getKeys() + ", " + rightInnerPageNode.getKeys());
//        System.out.println(leftInnerPageNode.getChildren() + ", " + rightInnerPageNode.getChildren());
//        System.out.println(leftInnerPageNode.getPageId() + ", " + rightInnerPageNode.getPageId());

        ByteArrayInputStream bis = null;
        ObjectInputStream in = null;
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        ObjectOutput out = null;

        // assign left and right inner nodes to different parents
        int i;
        for (i = 0; i < leftInnerPageNode.getChildren().size(); i++) {
            int pageId = leftInnerPageNode.getChildren().get(i);
            deserializeAndSerializePageNode(pageId, bis, in, bos, out, leftInnerPageNode);
        }

        for (i = 0; i < rightInnerPageNode.getChildren().size(); i++) {
            int pageId = rightInnerPageNode.getChildren().get(i);
            deserializeAndSerializePageNode(pageId, bis, in, bos, out, rightInnerPageNode);
        }

        if (leftInnerPageNode.isRootPageNode()) {
//            System.out.println("inner node is root node");

            leftInnerPageNode.setParentPageId(rootPageNode.getPageId());
            rightInnerPageNode.setParentPageId(rootPageNode.getPageId());

            rootPageNode = new BPlusTreeInnerPageNode<>(splitKey, leftInnerPageNode.getPageId(), rightInnerPageNode.getPageId(), rootPageNode.getPageId(), rootPageNode.getParentPageId(), MAXDEGREE);
            handleRootPageNode(newPage, leftPage, rightInnerPageNode, leftInnerPageNode, bos, out);
        } else {
            BPlusTreePageNode<K, V> parentPageNode = handleParentPageNode(splitKey, leftInnerPageNode, rightInnerPageNode, bis, in, bos, out);

            rightInnerPageNode.setParentPageId(leftInnerPageNode.getParentPageId());
            serializePageNode(newPage, bos, out, rightInnerPageNode);

            serializePageNode(leftPage, bos, out, leftInnerPageNode);

            if (parentPageNode.isOverSized()) {
                splitInnerNode((BPlusTreeInnerPageNode<K, V>) parentPageNode);
            }
        }
    }

    public V find(K key) {
        BPlusTreeLeafPageNode<K, V> targetPageNode = findHelper(rootPageId, key);
        return targetPageNode == null ? null : targetPageNode.getValue(key);
    }

    @SuppressWarnings("unchecked")
    private BPlusTreeLeafPageNode<K, V> findHelper(int rootPageId, K key) {
        BPlusTreePageNode<K, V> root = (BPlusTreePageNode<K, V>) deserializePageNode(rootPageId);

        if (root != null) {
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
        }

        return null;
    }

    /**
     * traverse the whole B+ tree using BFS
     */
    @SuppressWarnings("unchecked")
    public void traverseAllNodes() {
        Queue<Integer> queue = new LinkedList<>();
        if (!isEmpty()) {
            queue.add(rootPageId);
            while (!queue.isEmpty()) {
                int curPageId = queue.poll();
                BPlusTreePageNode<K, V> curPageNode = (BPlusTreePageNode<K, V>) deserializePageNode(curPageId);

                if (curPageNode != null) {
                    System.out.println(curPageNode.getKeys() + ", ");

                    if (!curPageNode.isLeafPageNode()) {
                        int i;
                        for (i = 0; i < ((BPlusTreeInnerPageNode<K, V>) curPageNode).getChildren().size(); i++) {
                            queue.add(((BPlusTreeInnerPageNode<K, V>) curPageNode).getChildren().get(i));
                        }
                    }
                }
            }
        }
    }

    /**
     *
     * @param pageId
     * @return
     */
    @SuppressWarnings("unchecked")
    private Object deserializePageNode(int pageId) {
        try {
            Page rootPage = bufferManager.fetchPage(pageId);
            ByteArrayInputStream bis = new ByteArrayInputStream(rootPage.getPageData());
            ObjectInputStream in = new ObjectInputStream(bis);
            return in.readObject();
        } catch (IOException | ClassNotFoundException e) {
            e.printStackTrace();
        }

        return null;
    }

    /**
     *
     * @param page
     * @param bis
     * @param in
     * @return
     */
    private Object deserializePageNode(Page page, ByteArrayInputStream bis, ObjectInputStream in) {
        try {
            bis = new ByteArrayInputStream(page.getPageData());
            in = new ObjectInputStream(bis);
            return in.readObject();
        } catch (IOException | ClassNotFoundException e) {
            e.printStackTrace();
        }

        return null;
    }

    /**
     *
     * @param pageId
     * @param bis
     * @param in
     * @param bos
     * @param out
     * @param bPlusTreePageNode
     */
    @SuppressWarnings("unchecked")
    private void deserializeAndSerializePageNode(int pageId,
                                                 ByteArrayInputStream bis,
                                                 ObjectInputStream in,
                                                 ByteArrayOutputStream bos,
                                                 ObjectOutput out,
                                                 BPlusTreePageNode<K, V> bPlusTreePageNode) {
        Page p = bufferManager.fetchPage(pageId);
        try {
            bis = new ByteArrayInputStream(p.getPageData());
            in = new ObjectInputStream(bis);
            BPlusTreePageNode<K, V> childPageNode = (BPlusTreePageNode<K, V>) in.readObject();
            childPageNode.setParentPageId(bPlusTreePageNode.getPageId());

            serializePageNodeHelper(bos, out, childPageNode);

            p.setPageData(bos.toByteArray());
            bufferManager.unpinPage(p.getPageId(), true);

        } catch (IOException | ClassNotFoundException e) {
            e.printStackTrace();
        }
    }

    /**
     *
     * @param page
     * @param bos
     * @param out
     * @param bPlusTreePageNode
     */
    private void serializePageNode(Page page, ByteArrayOutputStream bos, ObjectOutput out, BPlusTreePageNode<K, V> bPlusTreePageNode) {
        serializePageNodeHelper(bos, out, bPlusTreePageNode);
        page.setPageData(bos.toByteArray());
        bufferManager.unpinPage(page.getPageId(), true);
    }

    /**
     *
     * @param bos
     * @param out
     * @param bPlusTreePageNode
     */
    private void serializePageNodeHelper(ByteArrayOutputStream bos, ObjectOutput out, BPlusTreePageNode<K, V> bPlusTreePageNode) {
        try {
            bos.reset();
            out = new ObjectOutputStream(bos);
            out.writeObject(bPlusTreePageNode);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void handleRootPageNode(Page newPage, Page leftPage,
                                    BPlusTreePageNode<K, V> rightPageNode,
                                    BPlusTreePageNode<K, V> leftPageNode,
                                    ByteArrayOutputStream bos, ObjectOutput out) {
        serializePageNode(newPage, bos, out, rightPageNode);
        serializePageNode(leftPage, bos, out, leftPageNode);
        Page rootPage = bufferManager.fetchPage(rootPageNode.getPageId());
        serializePageNode(rootPage, bos, out, rootPageNode);
    }

    @SuppressWarnings("unchecked")
    private BPlusTreePageNode<K, V> handleParentPageNode(K splitKey,
                                      BPlusTreePageNode<K, V> leftPageNode,
                                      BPlusTreePageNode<K, V> rightPageNode,
                                      ByteArrayInputStream bis,
                                      ObjectInputStream in,
                                      ByteArrayOutputStream bos,
                                      ObjectOutput out) {
        Page parentPage = bufferManager.fetchPage(leftPageNode.getParentPageId());
        BPlusTreePageNode<K, V> parentPageNode = (BPlusTreePageNode<K, V>) deserializePageNode(parentPage, bis, in);
        if (parentPageNode != null) {
            ((BPlusTreeInnerPageNode<K, V>) parentPageNode).insertAndSort(splitKey, rightPageNode.getPageId());
            serializePageNode(parentPage, bos, out, parentPageNode);
        }
        return parentPageNode;
    }
}
