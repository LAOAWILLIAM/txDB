package txDB.storage.index;

import txDB.buffer.BufferManager;
import txDB.Config;
import txDB.storage.page.*;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;

/**
 * This class is a disk-based B+ tree
 */
public class BPlusTreeIndex<K extends Comparable<K>, V> {
    // TODO
    private BufferManager bufferManager;
    private int rootPageId;
    private static int MAXDEGREE;

    public BPlusTreeIndex(BufferManager bufferManager, int rootPageId, int maxDegree) {
        this.bufferManager = bufferManager;
        this.rootPageId = rootPageId;
        MAXDEGREE = maxDegree;
    }

    private boolean isEmpty() {
        return rootPageId == Config.INVALID_PAGE_ID;
    }

    public void insert(K key, V value) {
        if (isEmpty()) {
            startNewTree();
        }
    }

    private void startNewTree() {
        Page rootPage = bufferManager.newPage();
        BPlusTreeLeafPage bPlusTreeLeafPage = new BPlusTreeLeafPage();
        bPlusTreeLeafPage.setPageId(rootPage.getPageId());
        bPlusTreeLeafPage.setIndexPageType(BPlusTreePage.IndexPageType.LEAFPAGE);
        rootPageId = rootPage.getPageId();

        try {
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            ObjectOutput out = new ObjectOutputStream(bos);
            out.writeObject(bPlusTreeLeafPage);
            rootPage.setPageData(bos.toByteArray());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

//    private void insertHelper(Node<K, V> root, K key, V value) {
//        if (root.isLeafNode) {
//            ((LeafNode<K, V>) root).insertAndSort(key, value);
////            System.out.println("leaf node keys: " + root.keys);
//            if (root.isOverSized()) {
//                splitLeafNode((LeafNode<K, V>) root);
//            }
//        } else {
//            if (key.compareTo(root.keys.get(0)) < 0) {
//                insertHelper(((InnerNode<K, V>) root).children.get(0), key, value);
//            } else if (key.compareTo(root.keys.get(root.keys.size() - 1)) >= 0) {
//                insertHelper(((InnerNode<K, V>) root).children.get(((InnerNode<K, V>) root).children.size() - 1), key, value);
//            } else {
//                int i;
//                for (i = 1; i < root.keys.size(); i++) {
//                    if (((InnerNode<K, V>) root).keys.get(i).compareTo(key) > 0)
//                        insertHelper(((InnerNode<K, V>) root).children.get(i), key, value);
//                }
//            }
//        }
//    }
}
