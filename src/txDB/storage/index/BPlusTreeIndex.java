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
    private BPlusTreePageNode rootPageNode;

    public BPlusTreeIndex(BufferManager bufferManager, int rootPageId) {
        this.bufferManager = bufferManager;
        this.rootPageId = rootPageId;
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
        BPlusTreeLeafPageNode<K, V> bPlusTreeLeafPageNode = new BPlusTreeLeafPageNode<>(key, value, rootPage.getPageId(), Config.INVALID_PAGE_ID);
        rootPageId = rootPage.getPageId();

        try {
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            ObjectOutput out = new ObjectOutputStream(bos);
            out.writeObject(bPlusTreeLeafPageNode);
            byte[] pageData = bos.toByteArray();
            bPlusTreeLeafPageNode.setMaxSize(Config.PAGE_SIZE - pageData.length);
            rootPage.setPageData(pageData);
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

            if (bPlusTreePageNode.isLeafPage()) {
                ((BPlusTreeLeafPageNode<K, V>) bPlusTreePageNode).insertAndSort(key, value);
                if (bPlusTreePageNode.isOverSized()) {
                    splitLeafNode((BPlusTreeLeafPageNode<K, V>) bPlusTreePageNode);
                }
            } else {
                if (key.compareTo(bPlusTreePageNode.getKeys().get(0)) < 0) {
                    insertHelper(((BPlusTreeInnerPageNode<K, V>) bPlusTreePageNode).getChildren().get(0), key, value);
                } else if (key.compareTo(bPlusTreePageNode.getKeys().get(bPlusTreePageNode.getKeys().size() - 1)) >= 0) {
                    insertHelper(((BPlusTreeInnerPageNode<K, V>) bPlusTreePageNode).getChildren().get(((BPlusTreeInnerPageNode<K, V>) bPlusTreePageNode).getChildren().size() - 1), key, value);
                } else {
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
        // TODO: from needed to be done
        int from = 1, to = leftLeafPageNode.getKeys().size();
        K splitKey = leftLeafPageNode.getKeys().get(from);

        Page newPage = bufferManager.newPage();
        BPlusTreeLeafPageNode<K, V> rightLeafPageNode = new BPlusTreeLeafPageNode<>(
                leftLeafPageNode.getKeys().subList(from, to),
                leftLeafPageNode.getValues().subList(from, to),
                newPage.getPageId(),
                leftLeafPageNode.getParentPageId()
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
                BPlusTreeLeafPageNode<K, V> nextLeafPage = (BPlusTreeLeafPageNode<K, V>) in.readObject();
                nextLeafPage.setPrevPageId(rightLeafPageNode.getPageId());
            } catch (IOException | ClassNotFoundException e) {
                e.printStackTrace();
            }
            rightLeafPageNode.setNextPageId(leftLeafPageNode.getNextPageId());
        }
        rightLeafPageNode.setPrevPageId(leftLeafPageNode.getPageId());
        leftLeafPageNode.setNextPageId(rightLeafPageNode.getPageId());

        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        ObjectOutput out;

        if (rootPageNode.isLeafPage()) {
            // it is the first time of splitting
            rootPageNode = new BPlusTreeInnerPageNode<>(splitKey, leftLeafPageNode.getPageId(), rightLeafPageNode.getPageId(), rootPageNode.getPageId(), rootPageNode.getParentPageId());
            leftLeafPageNode.setParentPageId(rootPageId);
            rightLeafPageNode.setParentPageId(rootPageId);
            try {
                out = new ObjectOutputStream(bos);
                out.writeObject(rightLeafPageNode);
                newPage.setPageData(bos.toByteArray());

                out.writeObject(leftLeafPageNode);
                Page leftPage = bufferManager.fetchPage(leftLeafPageNode.getPageId());
                leftPage.setPageData(bos.toByteArray());

                out.writeObject(rootPageNode);
                Page rootPage = bufferManager.fetchPage(rootPageNode.getPageId());
                rootPage.setPageData(bos.toByteArray());
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
}
