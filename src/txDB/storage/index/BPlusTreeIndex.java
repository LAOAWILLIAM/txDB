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
    // TODO: thread-safe nneded
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
            startNewTree(key, value);
            return;
        }

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
        serializePageNode(rootPage, rootPageNode);
//        this.bufferManager.unpinPage(rootPageId, true);
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
                this.bufferManager.unpinPage(rootPageId, true);
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
                this.bufferManager.unpinPage(rootPageId, false);
            }
        } catch (IOException | ClassNotFoundException e) {
            e.printStackTrace();
        }
    }

    /**
     *
     * @param leftLeafPageNode
     */
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
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        ObjectOutput out = null;

        if (leftLeafPageNode.getNextPageId() != Config.INVALID_PAGE_ID) {
            Page nextPage = bufferManager.fetchPage(leftLeafPageNode.getNextPageId());
            try {
                bis = new ByteArrayInputStream(nextPage.getPageData());
                in = new ObjectInputStream(bis);
                BPlusTreeLeafPageNode<K, V> nextLeafPageNode = (BPlusTreeLeafPageNode<K, V>) in.readObject();
                nextLeafPageNode.setPrevPageId(rightLeafPageNode.getPageId());

                out = new ObjectOutputStream(bos);
                out.writeObject(nextLeafPageNode);
                nextPage.setPageData(bos.toByteArray());
                this.bufferManager.unpinPage(nextLeafPageNode.getPageId(), true);
            } catch (IOException | ClassNotFoundException e) {
                e.printStackTrace();
            }
            rightLeafPageNode.setNextPageId(leftLeafPageNode.getNextPageId());
        }
        rightLeafPageNode.setPrevPageId(leftLeafPageNode.getPageId());
        leftLeafPageNode.setNextPageId(rightLeafPageNode.getPageId());

        if (rootPageNode.isLeafPageNode()) {
//            System.out.println("root page node is leaf node");
            // it is the first time of splitting
            Page leftPage = bufferManager.newPage();
            leftLeafPageNode.setPageId(leftPage.getPageId());
            leftLeafPageNode.setParentPageId(rootPageId);
            rightLeafPageNode.setParentPageId(rootPageId);
            rootPageNode = new BPlusTreeInnerPageNode<>(splitKey, leftLeafPageNode.getPageId(), rightLeafPageNode.getPageId(), rootPageNode.getPageId(), rootPageNode.getParentPageId(), MAXDEGREE);
            handleRootPageNode(newPage, leftPage, rightLeafPageNode, leftLeafPageNode, bos, out);
//            bufferManager.unpinPage(newPage.getPageId(), true);
//            bufferManager.unpinPage(leftPage.getPageId(), true);
//            bufferManager.unpinPage(rootPageNode.getPageId(), true);
        } else {
            BPlusTreePageNode<K, V> parentPageNode = handleParentPageNode(splitKey, leftLeafPageNode, rightLeafPageNode, bis, in, bos, out);

            serializePageNode(newPage, bos, out, rightLeafPageNode);
//            bufferManager.unpinPage(newPage.getPageId(), true);

            Page leftPage = bufferManager.fetchPage(leftLeafPageNode.getPageId());
            serializePageNode(leftPage, bos, out, leftLeafPageNode);
//            bufferManager.unpinPage(leftPage.getPageId(), true);

            if (parentPageNode.isOverSized()) {
                splitInnerNode((BPlusTreeInnerPageNode<K, V>) parentPageNode);
            }

            bufferManager.unpinPage(parentPageNode.getPageId(), true);
        }
    }

    /**
     *
     * @param leftInnerPageNode
     */
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
//            bufferManager.unpinPage(newPage.getPageId(), true);
//            bufferManager.unpinPage(leftPage.getPageId(), true);
//            bufferManager.unpinPage(rootPageNode.getPageId(), true);
        } else {
            BPlusTreePageNode<K, V> parentPageNode = handleParentPageNode(splitKey, leftInnerPageNode, rightInnerPageNode, bis, in, bos, out);

            rightInnerPageNode.setParentPageId(leftInnerPageNode.getParentPageId());
            serializePageNode(newPage, bos, out, rightInnerPageNode);
//            bufferManager.unpinPage(newPage.getPageId(), true);

            serializePageNode(leftPage, bos, out, leftInnerPageNode);
//            bufferManager.unpinPage(leftPage.getPageId(), true);

            if (parentPageNode.isOverSized()) {
                splitInnerNode((BPlusTreeInnerPageNode<K, V>) parentPageNode);
            }

//            bufferManager.unpinPage(parentPageNode.getPageId(), true);
        }
    }

    /**
     *
     * @param key
     * @return
     */
    public V find(K key) {
        BPlusTreeLeafPageNode<K, V> targetPageNode = findHelper(rootPageId, key);
        return targetPageNode == null ? null : targetPageNode.getValue(key);
    }

    /**
     *
     * @param rootPageId
     * @param key
     * @return
     */
    @SuppressWarnings("unchecked")
    private BPlusTreeLeafPageNode<K, V> findHelper(int rootPageId, K key) {
        BPlusTreePageNode<K, V> root = (BPlusTreePageNode<K, V>) deserializePageNode(rootPageId);
        this.bufferManager.unpinPage(rootPageId, false);
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
     *
     * @param key
     */
    public void delete(K key) {
        if (rootPageId != Config.INVALID_PAGE_ID) {
            deleteHelper(rootPageId, key, -1);
        }
    }

    @SuppressWarnings("unchecked")
    private void deleteHelper(int rootPageId, K key, int keyIndex) {
        Page rootPage = bufferManager.fetchPage(rootPageId);
        BPlusTreePageNode<K, V> root = (BPlusTreePageNode<K, V>) deserializePageNode(rootPage);

        if (root != null) {
            if (root.isLeafPageNode()) {
//            System.out.println("key: " + key);
                ((BPlusTreeLeafPageNode<K, V>) root).remove(key);
                serializePageNode(rootPage, root);
                // -1 means root is both rootNode and leafNode, so we do not do redistribution
                if (root.isUnderSized() && keyIndex != -1) {
                    redistributeLeafNode((BPlusTreeLeafPageNode<K, V>) root, rootPage, keyIndex);
                }
                this.bufferManager.unpinPage(rootPageId, true);
            } else {
                if (key.compareTo(root.getKeys().get(0)) < 0) {
                    deleteHelper(((BPlusTreeInnerPageNode<K, V>) root).getChildren().get(0), key, 0);
                } else if (key.compareTo(root.getKeys().get(root.getKeys().size() - 1)) >= 0) {
                    deleteHelper(((BPlusTreeInnerPageNode<K, V>) root).getChildren().get(((BPlusTreeInnerPageNode<K, V>) root).getChildren().size() - 1), key, ((BPlusTreeInnerPageNode<K, V>) root).getChildren().size() - 1);
                } else {
                    int i;
                    for (i = 1; i < root.getKeys().size(); i++) {
                        if (root.getKeys().get(i).compareTo(key) > 0)
                            deleteHelper(((BPlusTreeInnerPageNode<K, V>) root).getChildren().get(i), key, i);
                    }
                }
                this.bufferManager.unpinPage(rootPageId, false);
            }
        }
    }

    @SuppressWarnings("unchecked")
    private void redistributeLeafNode(BPlusTreeLeafPageNode<K, V> bPlusTreeLeafPageNode, Page bPlusTreeLeafPage, int keyIndex) {
        Page bPlusTreeLeafPage1 = bufferManager.fetchPage(bPlusTreeLeafPageNode.getNextPageId());
        // leaf node is at the leftmost when keyIndex == 0
        if (keyIndex == 0) {
            BPlusTreeLeafPageNode<K, V> bPlusTreeLeafPageNode1 = (BPlusTreeLeafPageNode<K, V>) deserializePageNode(bPlusTreeLeafPage1);
            if (bPlusTreeLeafPageNode1 != null) redistributeLeafNodeHelper(bPlusTreeLeafPage, bPlusTreeLeafPageNode, bPlusTreeLeafPage1, bPlusTreeLeafPageNode1, keyIndex + 1);
        } else {
            BPlusTreeLeafPageNode<K, V> bPlusTreeLeafPageNode1 = (BPlusTreeLeafPageNode<K, V>) deserializePageNode(bPlusTreeLeafPageNode.getPrevPageId());
            if (bPlusTreeLeafPageNode1 != null) redistributeLeafNodeHelper(bPlusTreeLeafPage1, bPlusTreeLeafPageNode1, bPlusTreeLeafPage, bPlusTreeLeafPageNode, keyIndex);
        }
    }

    @SuppressWarnings("unchecked")
    private void redistributeLeafNodeHelper(Page page, BPlusTreeLeafPageNode<K, V> smallNode, Page page1, BPlusTreeLeafPageNode<K, V> largeNode, int keyIndex) {
        int totalSize = smallNode.getKeys().size() + largeNode.getKeys().size();
//        System.out.println("keyIndex: " + keyIndex);
//        System.out.println("totalSize: "+ totalSize);
        if (totalSize >= 2 * Math.round(MAXDEGREE / 2.0 - 1)) {
            /**
             * Reference: https://github.com/tiejian/database-hw2/blob/master/BPlusTree.java
             */
            // Store all keys and values from left to right
            ArrayList<K> keys = new ArrayList<>();
            ArrayList<V> vals = new ArrayList <>();
            keys.addAll(smallNode.getKeys());
            keys.addAll(largeNode.getKeys());
            vals.addAll(smallNode.getValues());
            vals.addAll(largeNode.getValues());

            int leftSize = totalSize / 2;

            smallNode.getKeys().clear();
            largeNode.getKeys().clear();
            smallNode.getValues().clear();
            largeNode.getValues().clear();

            // Add first half keys and values into left and rest into right
            smallNode.getKeys().addAll(keys.subList(0, leftSize));
            smallNode.getValues().addAll(vals.subList(0, leftSize));
            largeNode.getKeys().addAll(keys.subList(leftSize, keys.size()));
            largeNode.getValues().addAll(vals.subList(leftSize, vals.size()));

            serializePageNode(page, smallNode);
            serializePageNode(page1, largeNode);

            Page parentPage = bufferManager.fetchPage(smallNode.getParentPageId());
            BPlusTreeInnerPageNode<K, V> parentNode = (BPlusTreeInnerPageNode<K, V>) deserializePageNode(parentPage);
            if (parentNode != null) {
                parentNode.getKeys().set(keyIndex - 1, largeNode.getKeys().get(0));
                serializePageNode(parentPage, parentNode);
            }
        } else {
            mergeLeafNode(page, smallNode, page1, largeNode, keyIndex - 1);
        }
    }

    @SuppressWarnings("unchecked")
    private void redistributeInnerNode(Page parentPage, BPlusTreeInnerPageNode<K, V> parentNode) {
        int i, parentIndex = -1;
        Page grandParentPage = bufferManager.fetchPage(parentNode.getParentPageId());
        BPlusTreeInnerPageNode<K, V> grandParent = (BPlusTreeInnerPageNode<K, V>) deserializePageNode(grandParentPage);

        if (grandParent != null) {
            // find index of parent in grandparent's children list
            for (i = 0; i < grandParent.getChildren().size(); i++) {
                if (grandParent.getChildren().get(i) == parentNode.getPageId()) {
                    parentIndex = i;
                    break;
                }
            }

//        System.out.println("parentIndex: " + parentIndex);

            BPlusTreeInnerPageNode<K, V> sibling;
            if (parentIndex == 0) {
                Page siblingPage = bufferManager.fetchPage(grandParent.getChildren().get(parentIndex + 1));
                sibling = (BPlusTreeInnerPageNode<K, V>) deserializePageNode(siblingPage);
                if (sibling != null) redistributeInnerNodeHelper(parentPage, parentNode, siblingPage, sibling, parentIndex);
            } else {
                Page siblingPage = bufferManager.fetchPage(grandParent.getChildren().get(parentIndex - 1));
                sibling = (BPlusTreeInnerPageNode<K, V>) deserializePageNode(siblingPage);
                if (sibling != null) redistributeInnerNodeHelper(siblingPage, sibling, parentPage, parentNode, parentIndex - 1);
            }
        }
    }

    @SuppressWarnings("unchecked")
    private void redistributeInnerNodeHelper(Page page, BPlusTreeInnerPageNode<K, V> smallNode, Page page1, BPlusTreeInnerPageNode<K, V> largeNode, int parentIndex) {
        int totalSize = smallNode.getKeys().size() + largeNode.getKeys().size();
//        System.out.println("inner totalSize: " + totalSize);
        if (totalSize >= 2 * Math.round(MAXDEGREE / 2.0 - 1)) {
            Page parentPage = bufferManager.fetchPage(smallNode.getParentPageId());
            BPlusTreeInnerPageNode<K, V> parentNode = (BPlusTreeInnerPageNode<K, V>) deserializePageNode(parentPage);

            if (parentNode != null) {
                /**
                 * Reference: https://github.com/tiejian/database-hw2/blob/master/BPlusTree.java
                 */
                // Store all keys and values from left to right
                ArrayList<K> keys = new ArrayList<>();
                ArrayList<Integer> kids = new ArrayList<>();
                keys.addAll(smallNode.getKeys());
                keys.add(parentNode.getKeys().get(parentIndex));
                keys.addAll(largeNode.getKeys());
                kids.addAll(smallNode.getChildren());
                kids.addAll(largeNode.getChildren());

                // Get the index of the new parent key
                int newIndex = keys.size() / 2;
                if (keys.size() % 2 == 0) {
                    newIndex -= 1;
                }
                parentNode.getKeys().set(parentIndex, keys.get(newIndex));

                smallNode.getKeys().clear();
                largeNode.getKeys().clear();
                smallNode.getChildren().clear();
                largeNode.getChildren().clear();

                // Add first half keys and values into left and rest into right
                smallNode.getKeys().addAll(keys.subList(0, newIndex));
                largeNode.getKeys().addAll(keys.subList(newIndex + 1, keys.size()));
                smallNode.getChildren().addAll(kids.subList(0, newIndex + 1));
                largeNode.getChildren().addAll(kids.subList(newIndex + 1, kids.size()));

                serializePageNode(page, smallNode);
                serializePageNode(page1, largeNode);
                serializePageNode(parentPage, parentNode);
            }
        } else {
            mergeInnerNode(page, smallNode, page1, largeNode, parentIndex);
        }
    }

    @SuppressWarnings("unchecked")
    private void mergeLeafNode(Page page, BPlusTreeLeafPageNode<K, V> smallNode, Page page1, BPlusTreeLeafPageNode<K, V> largeNode, int keyIndex) {
        smallNode.getKeys().addAll(largeNode.getKeys());
        smallNode.getValues().addAll(largeNode.getValues());

        smallNode.setNextPageId(largeNode.getNextPageId());
        if (largeNode.getNextPageId() != Config.INVALID_PAGE_ID) {
            Page nextPage = bufferManager.fetchPage(largeNode.getNextPageId());
            BPlusTreeLeafPageNode<K, V> nextNode = (BPlusTreeLeafPageNode<K, V>) deserializePageNode(nextPage);
            if (nextNode != null) nextNode.setPrevPageId(smallNode.getPageId());
            serializePageNode(nextPage, nextNode);
        }

        Page parentPage = bufferManager.fetchPage(smallNode.getParentPageId());
        BPlusTreeInnerPageNode<K, V> parentNode = (BPlusTreeInnerPageNode<K, V>) deserializePageNode(parentPage);
        if (parentNode != null) {
//            System.out.println(parentNode.getChildren().get(keyIndex + 1) + ", " + largeNode.getPageId());
            bufferManager.deletePage(parentNode.getChildren().get(keyIndex + 1));
            parentNode.remove(keyIndex);

            serializePageNode(page, smallNode);
            serializePageNode(parentPage, parentNode);

            if (!parentNode.isRootPageNode() && parentNode.isUnderSized()) {
                redistributeInnerNode(parentPage, parentNode);
            }

            if (parentNode.isRootPageNode() && parentNode.getKeys().size() == 0) {
                rootPageNode = smallNode;
                rootPageId = smallNode.getPageId();
            }
        }
    }

    @SuppressWarnings("unchecked")
    private void mergeInnerNode(Page page, BPlusTreeInnerPageNode<K, V> smallNode, Page page1, BPlusTreeInnerPageNode<K, V> largeNode, int parentIndex) {
//        System.out.println("inner parentIndex: " + parentIndex);
        Page parentPage = bufferManager.fetchPage(smallNode.getParentPageId());
        BPlusTreeInnerPageNode<K, V> parentNode = (BPlusTreeInnerPageNode<K, V>) deserializePageNode(parentPage);

        if (parentNode != null) {
//            System.out.println(parentNode.getKeys());
            smallNode.getKeys().add(parentNode.getKeys().get(parentIndex));
            smallNode.getKeys().addAll(largeNode.getKeys());
            smallNode.getChildren().addAll(largeNode.getChildren());

            bufferManager.deletePage(parentNode.getChildren().get(parentIndex + 1));
            parentNode.remove(parentIndex);

            serializePageNode(page, smallNode);
            serializePageNode(parentPage, parentNode);

//            System.out.println("inner parent keys: " + parent.keys);

            if (!parentNode.isRootPageNode() && parentNode.isUnderSized()) {
                redistributeInnerNode(parentPage, parentNode);
            }

            if (parentNode.isRootPageNode() && parentNode.getKeys().size() == 0) {
                rootPageNode = smallNode;
                rootPageId = smallNode.getPageId();
                smallNode.setParentPageId(Config.INVALID_PAGE_ID);
                serializePageNode(page, smallNode);
            }
        }
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

                this.bufferManager.unpinPage(curPageId, false);
            }
        }
    }

    /**
     * traverse only leaf nodes in B+ tree
     */
    @SuppressWarnings("unchecked")
    public void traverseLeafNodes() {
        int curPageId = rootPageId;
        BPlusTreeLeafPageNode<K, V> curLeafNode = traverseLeafNodesHelper(curPageId);

        // curPageId = -1 will cause diskManager Negative Position Exception
        while (curPageId != Config.INVALID_PAGE_ID) {
            if (curPageId != rootPageId)
                curLeafNode = (BPlusTreeLeafPageNode<K, V>) deserializePageNode(curPageId);
            if (curLeafNode != null) {
                System.out.println(curLeafNode.getKeys() + ", " + curLeafNode.getValues());
                curPageId = curLeafNode.getNextPageId();
            } else break;

            bufferManager.unpinPage(curPageId, false);
        }
    }

    /**
     * return the first leaf node using leftmost DFS
     */
    @SuppressWarnings("unchecked")
    private BPlusTreeLeafPageNode<K, V> traverseLeafNodesHelper(int rootPageId) {
        BPlusTreePageNode<K, V> root = (BPlusTreePageNode<K, V>) deserializePageNode(rootPageId);
        if (root == null) return null;
        else if (root.isLeafPageNode()) return ((BPlusTreeLeafPageNode<K, V>) root);
        else {
            return traverseLeafNodesHelper(((BPlusTreeInnerPageNode<K, V>) root).getChildren().get(0));
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
     * @return
     */
    private Object deserializePageNode(Page page) {
        try {
            ByteArrayInputStream bis = new ByteArrayInputStream(page.getPageData());
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
     */
    private void serializePageNode(Page page, BPlusTreePageNode<K, V> pageNode) {
        try {
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            ObjectOutput out = new ObjectOutputStream(bos);
            out.writeObject(pageNode);
            page.setPageData(bos.toByteArray());
            bufferManager.unpinPage(pageNode.getPageId(), true);
        } catch (IOException e) {
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

    /**
     *
     * @param newPage
     * @param leftPage
     * @param rightPageNode
     * @param leftPageNode
     * @param bos
     * @param out
     */
    private void handleRootPageNode(Page newPage, Page leftPage,
                                    BPlusTreePageNode<K, V> rightPageNode,
                                    BPlusTreePageNode<K, V> leftPageNode,
                                    ByteArrayOutputStream bos, ObjectOutput out) {
        serializePageNode(newPage, bos, out, rightPageNode);
        serializePageNode(leftPage, bos, out, leftPageNode);
        Page rootPage = bufferManager.fetchPage(rootPageNode.getPageId());
        serializePageNode(rootPage, bos, out, rootPageNode);
        bufferManager.unpinPage(rootPage.getPageId(), true);
    }

    /**
     *
     * @param splitKey
     * @param leftPageNode
     * @param rightPageNode
     * @param bis
     * @param in
     * @param bos
     * @param out
     * @return
     */
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
            bufferManager.unpinPage(parentPageNode.getPageId(), true);
        }
        return parentPageNode;
    }
}
