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
    // TODO: thread-safe needed, here I choose optimistic assumption
    private BufferManager bufferManager;
    private int rootPageId;
    private BPlusTreePageNode<K, V> rootPageNode;
    private final int MAXDEGREE;
    private final int MAXDEGREE1;

    @SuppressWarnings("unchecked")
    public BPlusTreeIndex(BufferManager bufferManager, int rootPageId, int maxDegree, int maxDegree1) {
        this.bufferManager = bufferManager;
        this.rootPageId = rootPageId;
        MAXDEGREE = maxDegree;
        MAXDEGREE1 = maxDegree1;
        if (!isEmpty()) {
            rootPageNode = (BPlusTreePageNode) deserializePageNode(rootPageId);
        } else {
            startNewTree();
        }
    }

    public int getRootPageId() {
        return this.rootPageId;
    }

    /**
     *
     * @return
     */
    private boolean isEmpty() {
        return rootPageId == Config.INVALID_PAGE_ID;
    }

    private boolean isInitialized() {
        Page rootPage = bufferManager.fetchPage(rootPageId);
        byte[] rootPageData = rootPage.getPageData();
        for (byte b : rootPageData) {
            if (b != 0) {
                bufferManager.unpinPage(rootPageId, false);
                return true;
            }
        }
        bufferManager.unpinPage(rootPageId, false);
        return false;
    }

    /**
     *
     * @param key
     * @param value
     */
    public void insert(K key, V value) {
//        if (isEmpty()) {
        if (!isInitialized()) {
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
        Page rootPage = bufferManager.fetchPage(rootPageId);
        rootPageNode = new BPlusTreeLeafPageNode<>(key, value, rootPageId, Config.INVALID_PAGE_ID, MAXDEGREE);
        ((BPlusTreeLeafPageNode<K, V>) rootPageNode).insertAndSort(key, value);
        serializePageNode(rootPage, rootPageNode);
    }

    private void startNewTree() {
        Page rootPage = bufferManager.newPage();
        rootPageId = rootPage.getPageId();
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
            BPlusTreePageNode<K, V> bPlusTreePageNode = (BPlusTreePageNode<K, V>) deserializePageNode(rootPage);

//            assert bPlusTreePageNode != null;
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
//                    int i;
//                    for (i = 1; i < bPlusTreePageNode.getKeys().size(); i++) {
//                        if (bPlusTreePageNode.getKeys().get(i).compareTo(key) > 0) {
//                            insertHelper(((BPlusTreeInnerPageNode<K, V>) bPlusTreePageNode).getChildren().get(i), key, value);
//                            break;
//                        }
//                    }

                    // Binary search
                    int start = 1, end = bPlusTreePageNode.getKeys().size() - 2;
                    while (start <= end) {
                        int mid = start + (end - start) / 2;
                        if (bPlusTreePageNode.getKeys().get(mid).compareTo(key) > 0) {
                            if (bPlusTreePageNode.getKeys().get(mid - 1).compareTo(key) <= 0) {
                                insertHelper(((BPlusTreeInnerPageNode<K, V>) bPlusTreePageNode).getChildren().get(mid), key, value);
                                break;
                            } else {
                                end = mid - 1;
                            }
                        } else if (bPlusTreePageNode.getKeys().get(mid).compareTo(key) == 0) {
                            insertHelper(((BPlusTreeInnerPageNode<K, V>) bPlusTreePageNode).getChildren().get(mid), key, value);
                            break;
                        } else {
                            start = mid + 1;
                        }
                    }
                }
                this.bufferManager.unpinPage(rootPageId, false);
            }
        } catch (IOException e) {
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

        if (leftLeafPageNode.getNextPageId() != Config.INVALID_PAGE_ID) {
            Page nextPage = bufferManager.fetchPage(leftLeafPageNode.getNextPageId());
            BPlusTreeLeafPageNode<K, V> nextLeafPageNode = (BPlusTreeLeafPageNode<K, V>) deserializePageNode(nextPage);
//            assert nextLeafPageNode != null;
            if (nextLeafPageNode != null) nextLeafPageNode.setPrevPageId(rightLeafPageNode.getPageId());

            serializePageNode(nextPage, nextLeafPageNode);
            rightLeafPageNode.setNextPageId(leftLeafPageNode.getNextPageId());
        }
//        System.out.println();
        rightLeafPageNode.setPrevPageId(leftLeafPageNode.getPageId());
        leftLeafPageNode.setNextPageId(rightLeafPageNode.getPageId());

        if (rootPageNode.isLeafPageNode()) {
//            System.out.println("root page node is leaf node");
            // it is the first time of splitting
            Page leftPage = bufferManager.newPage();
            leftLeafPageNode.setPageId(leftPage.getPageId());
            leftLeafPageNode.setParentPageId(rootPageId);
            rightLeafPageNode.setParentPageId(rootPageId);
            rightLeafPageNode.setPrevPageId(leftLeafPageNode.getPageId());
//            System.out.println("left page: " + leftLeafPageNode.getPageId() + ", left page prev: " + leftLeafPageNode.getPrevPageId() + ", left page next: " + leftLeafPageNode.getNextPageId());
//            System.out.println("right page: " + rightLeafPageNode.getPageId() + ", right page prev: " + rightLeafPageNode.getPrevPageId() + ", right page next: " + rightLeafPageNode.getNextPageId());
            rootPageNode = new BPlusTreeInnerPageNode<>(splitKey, leftLeafPageNode.getPageId(), rightLeafPageNode.getPageId(), rootPageNode.getPageId(), rootPageNode.getParentPageId(), MAXDEGREE1);
            handleRootPageNode(newPage, leftPage, rightLeafPageNode, leftLeafPageNode);
//            bufferManager.unpinPage(newPage.getPageId(), true);
//            bufferManager.unpinPage(leftPage.getPageId(), true);
//            bufferManager.unpinPage(rootPageNode.getPageId(), true);
        } else {
            BPlusTreePageNode<K, V> parentPageNode = handleParentPageNode(splitKey, leftLeafPageNode, rightLeafPageNode);

            serializePageNode2(newPage, rightLeafPageNode);
//            bufferManager.unpinPage(newPage.getPageId(), true);

            Page leftPage = bufferManager.fetchPage(leftLeafPageNode.getPageId());
            serializePageNode2(leftPage, leftLeafPageNode);
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
        int from = MAXDEGREE1 / 2, keysTo = leftInnerPageNode.getKeys().size(), childrenTo = leftInnerPageNode.getChildren().size();
        K splitKey = leftInnerPageNode.getKeys().get(from);

        Page newPage = bufferManager.newPage();
        BPlusTreeInnerPageNode<K, V> rightInnerPageNode = new BPlusTreeInnerPageNode<>(
                leftInnerPageNode.getKeys().subList(from + 1, keysTo),
                leftInnerPageNode.getChildren().subList(from + 1, childrenTo),
                newPage.getPageId(),
                leftInnerPageNode.getParentPageId(),
                MAXDEGREE1
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

        // assign left and right inner nodes to different parents
        int i;
        for (i = 0; i < leftInnerPageNode.getChildren().size(); i++) {
            int pageId = leftInnerPageNode.getChildren().get(i);
            deserializeAndSerializePageNode(pageId, leftInnerPageNode);
        }

        for (i = 0; i < rightInnerPageNode.getChildren().size(); i++) {
            int pageId = rightInnerPageNode.getChildren().get(i);
            deserializeAndSerializePageNode(pageId, rightInnerPageNode);
        }

        if (leftInnerPageNode.isRootPageNode()) {
//            System.out.println("inner node is root node");

            leftInnerPageNode.setParentPageId(rootPageNode.getPageId());
            rightInnerPageNode.setParentPageId(rootPageNode.getPageId());

            rootPageNode = new BPlusTreeInnerPageNode<>(splitKey, leftInnerPageNode.getPageId(), rightInnerPageNode.getPageId(), rootPageNode.getPageId(), rootPageNode.getParentPageId(), MAXDEGREE1);
            handleRootPageNode(newPage, leftPage, rightInnerPageNode, leftInnerPageNode);
//            bufferManager.unpinPage(newPage.getPageId(), true);
//            bufferManager.unpinPage(leftPage.getPageId(), true);
//            bufferManager.unpinPage(rootPageNode.getPageId(), true);
        } else {
            BPlusTreePageNode<K, V> parentPageNode = handleParentPageNode(splitKey, leftInnerPageNode, rightInnerPageNode);

            rightInnerPageNode.setParentPageId(leftInnerPageNode.getParentPageId());
            serializePageNode2(newPage, rightInnerPageNode);
//            bufferManager.unpinPage(newPage.getPageId(), true);

            serializePageNode2(leftPage, leftInnerPageNode);
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
//        assert root != null;
        if (root != null) {
            if (root.getPageId() == Config.INVALID_PAGE_ID) return null;
            else if (root.isLeafPageNode()) return ((BPlusTreeLeafPageNode<K, V>) root);
            else {
                if (key.compareTo(root.getKeys().get(0)) < 0) {
                    return findHelper(((BPlusTreeInnerPageNode<K, V>) root).getChildren().get(0), key);
                } else if (key.compareTo(root.getKeys().get(root.getKeys().size() - 1)) >= 0) {
                    return findHelper(((BPlusTreeInnerPageNode<K, V>) root).getChildren().get(((BPlusTreeInnerPageNode<K, V>) root).getChildren().size() - 1), key);
                } else {
                    int i;
                    for (i = 1; i < root.getKeys().size(); i++) {
                        if (root.getKeys().get(i).compareTo(key) > 0)
                            return findHelper(((BPlusTreeInnerPageNode<K, V>) root).getChildren().get(i), key);
                    }
                    // TODO: bug in Binary Search
//                    int start = 1, end = root.getKeys().size() - 2;
//                    while (start <= end) {
//                        int mid = start + (end - start) / 2;
//                        if (root.getKeys().get(mid).compareTo(key) > 0) {
//                            if (root.getKeys().get(mid - 1).compareTo(key) <= 0) {
//                                return findHelper(((BPlusTreeInnerPageNode<K, V>) root).getChildren().get(mid), key);
//                            } else {
//                                end = mid - 1;
//                            }
//                        } else if (root.getKeys().get(mid).compareTo(key) == 0) {
//                            return findHelper(((BPlusTreeInnerPageNode<K, V>) root).getChildren().get(mid), key);
//                        } else {
//                            start = mid + 1;
//                        }
//                    }
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

    /**
     *
     * @param rootPageId
     * @param key
     * @param keyIndex
     */
    @SuppressWarnings("unchecked")
    private void deleteHelper(int rootPageId, K key, int keyIndex) {
        Page rootPage = bufferManager.fetchPage(rootPageId);
        BPlusTreePageNode<K, V> root = (BPlusTreePageNode<K, V>) deserializePageNode(rootPage);

//        assert root != null;
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
                        if (root.getKeys().get(i).compareTo(key) > 0) {
                            deleteHelper(((BPlusTreeInnerPageNode<K, V>) root).getChildren().get(i), key, i);
                            break;
                        }
                    }
                }
                this.bufferManager.unpinPage(rootPageId, false);
            }
        }
    }

    /**
     *
     * @param bPlusTreeLeafPageNode
     * @param bPlusTreeLeafPage
     * @param keyIndex
     */
    @SuppressWarnings("unchecked")
    private void redistributeLeafNode(BPlusTreeLeafPageNode<K, V> bPlusTreeLeafPageNode, Page bPlusTreeLeafPage, int keyIndex) {
        Page bPlusTreeLeafPage1 = bufferManager.fetchPage(bPlusTreeLeafPageNode.getNextPageId());
        // leaf node is at the leftmost when keyIndex == 0
        if (keyIndex == 0) {
            BPlusTreeLeafPageNode<K, V> bPlusTreeLeafPageNode1 = (BPlusTreeLeafPageNode<K, V>) deserializePageNode(bPlusTreeLeafPage1);
//            assert bPlusTreeLeafPageNode1 != null;
            if (bPlusTreeLeafPageNode1 != null) redistributeLeafNodeHelper(bPlusTreeLeafPage, bPlusTreeLeafPageNode, bPlusTreeLeafPage1, bPlusTreeLeafPageNode1, keyIndex + 1);
        } else {
            BPlusTreeLeafPageNode<K, V> bPlusTreeLeafPageNode1 = (BPlusTreeLeafPageNode<K, V>) deserializePageNode(bPlusTreeLeafPageNode.getPrevPageId());
//            assert bPlusTreeLeafPageNode1 != null;
            if (bPlusTreeLeafPageNode1 != null) redistributeLeafNodeHelper(bPlusTreeLeafPage1, bPlusTreeLeafPageNode1, bPlusTreeLeafPage, bPlusTreeLeafPageNode, keyIndex);
        }
    }

    /**
     *
     * @param page
     * @param smallNode
     * @param page1
     * @param largeNode
     * @param keyIndex
     */
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
//            assert parentNode != null;
            if (parentNode != null) {
                parentNode.getKeys().set(keyIndex - 1, largeNode.getKeys().get(0));
                serializePageNode(parentPage, parentNode);
            }
        } else {
            mergeLeafNode(page, smallNode, page1, largeNode, keyIndex - 1);
        }
    }

    /**
     *
     * @param parentPage
     * @param parentNode
     */
    @SuppressWarnings("unchecked")
    private void redistributeInnerNode(Page parentPage, BPlusTreeInnerPageNode<K, V> parentNode) {
        int i, parentIndex = -1;
        Page grandParentPage = bufferManager.fetchPage(parentNode.getParentPageId());
        BPlusTreeInnerPageNode<K, V> grandParent = (BPlusTreeInnerPageNode<K, V>) deserializePageNode(grandParentPage);

//        assert grandParent != null;
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
//                assert sibling != null;
                if (sibling != null) redistributeInnerNodeHelper(parentPage, parentNode, siblingPage, sibling, parentIndex);
            } else {
                Page siblingPage = bufferManager.fetchPage(grandParent.getChildren().get(parentIndex - 1));
                sibling = (BPlusTreeInnerPageNode<K, V>) deserializePageNode(siblingPage);
//                assert sibling != null;
                if (sibling != null) redistributeInnerNodeHelper(siblingPage, sibling, parentPage, parentNode, parentIndex - 1);
            }
        }
    }

    /**
     *
     * @param page
     * @param smallNode
     * @param page1
     * @param largeNode
     * @param parentIndex
     */
    @SuppressWarnings("unchecked")
    private void redistributeInnerNodeHelper(Page page, BPlusTreeInnerPageNode<K, V> smallNode, Page page1, BPlusTreeInnerPageNode<K, V> largeNode, int parentIndex) {
        int totalSize = smallNode.getKeys().size() + largeNode.getKeys().size();
//        System.out.println("inner totalSize: " + totalSize);
        if (totalSize >= 2 * Math.round(MAXDEGREE1 / 2.0 - 1)) {
            Page parentPage = bufferManager.fetchPage(smallNode.getParentPageId());
            BPlusTreeInnerPageNode<K, V> parentNode = (BPlusTreeInnerPageNode<K, V>) deserializePageNode(parentPage);

//            assert parentNode != null;
            if (parentNode != null) {
                /**
                 * Reference: https://github.com/tiejian/database-hw2/blob/master/BPlusTree.java
                 */
                // Store all keys and values from left to right
                ArrayList<Integer> kids = new ArrayList<>();
                ArrayList<K> keys = new ArrayList<>(smallNode.getKeys());
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

    /**
     *
     * @param page
     * @param smallNode
     * @param page1
     * @param largeNode
     * @param keyIndex
     */
    @SuppressWarnings("unchecked")
    private void mergeLeafNode(Page page, BPlusTreeLeafPageNode<K, V> smallNode, Page page1, BPlusTreeLeafPageNode<K, V> largeNode, int keyIndex) {
        smallNode.getKeys().addAll(largeNode.getKeys());
        smallNode.getValues().addAll(largeNode.getValues());

        smallNode.setNextPageId(largeNode.getNextPageId());
        if (largeNode.getNextPageId() != Config.INVALID_PAGE_ID) {
            Page nextPage = bufferManager.fetchPage(largeNode.getNextPageId());
            BPlusTreeLeafPageNode<K, V> nextNode = (BPlusTreeLeafPageNode<K, V>) deserializePageNode(nextPage);
//            assert nextNode != null;
            if (nextNode != null) nextNode.setPrevPageId(smallNode.getPageId());
            serializePageNode(nextPage, nextNode);
        }

        Page parentPage = bufferManager.fetchPage(smallNode.getParentPageId());
        BPlusTreeInnerPageNode<K, V> parentNode = (BPlusTreeInnerPageNode<K, V>) deserializePageNode(parentPage);
//        assert parentNode != null;
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

    /**
     *
     * @param page
     * @param smallNode
     * @param page1
     * @param largeNode
     * @param parentIndex
     */
    @SuppressWarnings("unchecked")
    private void mergeInnerNode(Page page, BPlusTreeInnerPageNode<K, V> smallNode, Page page1, BPlusTreeInnerPageNode<K, V> largeNode, int parentIndex) {
//        System.out.println("inner parentIndex: " + parentIndex);
        Page parentPage = bufferManager.fetchPage(smallNode.getParentPageId());
        BPlusTreeInnerPageNode<K, V> parentNode = (BPlusTreeInnerPageNode<K, V>) deserializePageNode(parentPage);

//        assert parentNode != null;
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

//                assert curPageNode != null;
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
//        BPlusTreeLeafPageNode<K, V> curLeafNode = findHelper(rootPageId, key);

        // curPageId = -1 will cause diskManager Negative Position Exception
        while (curPageId != Config.INVALID_PAGE_ID) {
            curLeafNode = (BPlusTreeLeafPageNode<K, V>) deserializePageNode(curPageId);
//            assert curLeafNode != null;
            if (curLeafNode != null) {
                System.out.println(curLeafNode.getKeys() + ", " + curLeafNode.getValues());
                bufferManager.unpinPage(curPageId, false);
                curPageId = curLeafNode.getNextPageId();
//                curPageId = curLeafNode.getPrevPageId();
            } else break;

//            bufferManager.unpinPage(curPageId, false);
        }
    }

    /**
     * return the first leaf node using leftmost DFS
     * @param rootPageId
     * @return
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
     * range scanning through leaf nodes, e.g., select * from table0 where col0 > 50;
     * @param key
     * @param isLarger
     * @return
     */
    @SuppressWarnings("unchecked")
    public List<V> scanLeafNode(K key, boolean isLarger) {
        ArrayList<V> res = new ArrayList<>();
        BPlusTreeLeafPageNode<K, V> curLeafPageNode = findHelper(rootPageId, key);

        if (curLeafPageNode != null) {
            int startIndex = curLeafPageNode.getValueIndex(key), curPageId = curLeafPageNode.getPageId();
//            System.out.println(curPageId);
            if (isLarger) {
                res.addAll(curLeafPageNode.getValues().subList(startIndex, curLeafPageNode.getValues().size()));
                if (curLeafPageNode.getNextPageId() != Config.INVALID_PAGE_ID)
                    curLeafPageNode = (BPlusTreeLeafPageNode<K, V>) deserializePageNode(curLeafPageNode.getNextPageId());
                else curLeafPageNode = null;
                bufferManager.unpinPage(curPageId, false);
                while (curLeafPageNode != null) {
                    res.addAll(curLeafPageNode.getValues());
                    curPageId = curLeafPageNode.getPageId();
//                    System.out.println("cur: " + curPageId + ", next: " + curLeafPageNode.getNextPageId());
                    if (curLeafPageNode.getNextPageId() != Config.INVALID_PAGE_ID)
                        curLeafPageNode = (BPlusTreeLeafPageNode<K, V>) deserializePageNode(curLeafPageNode.getNextPageId());
                    else curLeafPageNode = null;
                    bufferManager.unpinPage(curPageId, false);
                }
            } else {
                ArrayList<V> tmp = new ArrayList<>(curLeafPageNode.getValues().subList(startIndex, curLeafPageNode.getValues().size()));
                Collections.reverse(tmp);
                res.addAll(tmp);
                if (curLeafPageNode.getPrevPageId() != Config.INVALID_PAGE_ID)
                    curLeafPageNode = (BPlusTreeLeafPageNode<K, V>) deserializePageNode(curLeafPageNode.getPrevPageId());
                else curLeafPageNode = null;
                bufferManager.unpinPage(curPageId, false);
                while (curLeafPageNode != null) {
                    tmp = new ArrayList<>(curLeafPageNode.getValues());
                    Collections.reverse(tmp);
                    res.addAll(tmp);
                    curPageId = curLeafPageNode.getPageId();
//                    System.out.println("cur: " + curPageId + ", prev: " + curLeafPageNode.getPrevPageId());
                    if (curLeafPageNode.getPrevPageId() != Config.INVALID_PAGE_ID)
                        curLeafPageNode = (BPlusTreeLeafPageNode<K, V>) deserializePageNode(curLeafPageNode.getPrevPageId());
                    else curLeafPageNode = null;
                    bufferManager.unpinPage(curPageId, false);
                }
            }
        }

        return res;
    }

    /************************************* serialize and deserialize functions ************************************/

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
     * @param pageId
     * @param bPlusTreePageNode
     */
    @SuppressWarnings("unchecked")
    private void deserializeAndSerializePageNode(int pageId,
                                                 BPlusTreePageNode<K, V> bPlusTreePageNode) {
        Page p = bufferManager.fetchPage(pageId);
        try {
            ByteArrayInputStream bis = new ByteArrayInputStream(p.getPageData());
            ObjectInputStream in = new ObjectInputStream(bis);
            BPlusTreePageNode<K, V> childPageNode = (BPlusTreePageNode<K, V>) in.readObject();
            childPageNode.setParentPageId(bPlusTreePageNode.getPageId());

            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            ObjectOutput out = new ObjectOutputStream(bos);
            out.writeObject(childPageNode);

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
     * @param bPlusTreePageNode
     */
    private void serializePageNode2(Page page, BPlusTreePageNode<K, V> bPlusTreePageNode) {
        try {
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            ObjectOutputStream out = new ObjectOutputStream(bos);
            out.writeObject(bPlusTreePageNode);
            page.setPageData(bos.toByteArray());
            bufferManager.unpinPage(page.getPageId(), true);
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
     */
    private void handleRootPageNode(Page newPage, Page leftPage,
                                    BPlusTreePageNode<K, V> rightPageNode,
                                    BPlusTreePageNode<K, V> leftPageNode) {
        serializePageNode2(newPage, rightPageNode);
        serializePageNode2(leftPage, leftPageNode);
        Page rootPage = bufferManager.fetchPage(rootPageNode.getPageId());
        serializePageNode2(rootPage, rootPageNode);
        bufferManager.unpinPage(rootPage.getPageId(), true);
    }

    /**
     *
     * @param splitKey
     * @param leftPageNode
     * @param rightPageNode
     * @return
     */
    @SuppressWarnings("unchecked")
    private BPlusTreePageNode<K, V> handleParentPageNode(K splitKey,
                                                         BPlusTreePageNode<K, V> leftPageNode,
                                                         BPlusTreePageNode<K, V> rightPageNode) {
        Page parentPage = bufferManager.fetchPage(leftPageNode.getParentPageId());
        BPlusTreePageNode<K, V> parentPageNode = (BPlusTreePageNode<K, V>) deserializePageNode(parentPage);
//        assert parentPageNode != null;
        if (parentPageNode != null) {
            ((BPlusTreeInnerPageNode<K, V>) parentPageNode).insertAndSort(splitKey, rightPageNode.getPageId());
            serializePageNode2(parentPage, parentPageNode);
            bufferManager.unpinPage(parentPageNode.getPageId(), true);
        }
        return parentPageNode;
    }
}
