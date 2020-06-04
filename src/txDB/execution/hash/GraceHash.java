package txDB.execution.hash;

import txDB.Config;
import txDB.buffer.BufferManager;
import txDB.storage.page.GraceHashBucketPage;
import txDB.storage.page.GraceHashHeaderPage;
import txDB.storage.page.Page;

import java.io.*;

/**
 * It is a on-disk grace hash table
 * @param <K>
 * @param <V>
 */
public class GraceHash<K extends Comparable<K>, V> {
    // TODO: refactor needed
    private int bucketSize;
    private BufferManager bufferManager;
    private int headerPageId;
    private GraceHashHeaderPage<K, V> graceHashHeaderPage;

    // first hash
    public GraceHash(int bucketSize, BufferManager bufferManager) {
        this.bucketSize = bucketSize;
        this.bufferManager = bufferManager;
        Page page = this.bufferManager.newPage();
        this.headerPageId = page.getPageId();
        graceHashHeaderPage = new GraceHashHeaderPage<>(page.getPageId(), bucketSize);
        try {
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            ObjectOutput out = new ObjectOutputStream(bos);
            out.writeObject(graceHashHeaderPage);
            page.setPageData(bos.toByteArray());
            System.out.println(bos.toByteArray().length);
            bufferManager.unpinPage(graceHashHeaderPage.getPageId(), true);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @SuppressWarnings("unchecked")
    public void insert(K key, V value) {
        int index = hash(key);
        if (graceHashHeaderPage.getHashArr()[index] == Config.INVALID_PAGE_ID) {
//            System.out.println(key.toString() + ": create a new index");
            Page page = bufferManager.newPage();
            graceHashHeaderPage.getHashArr()[index] = page.getPageId();
//            System.out.println(key.toString() + ": create new bucket");
            GraceHashBucketPage<K, V> curGraceHashBucketPage = new GraceHashBucketPage<>(page.getPageId(), 100);
            while (!curGraceHashBucketPage.insert(key, value)) {
//                System.out.println(key.toString() + ": create new bucket in while");
                page = bufferManager.newPage();
                curGraceHashBucketPage.setNextPageId(page.getPageId());
                curGraceHashBucketPage = new GraceHashBucketPage<>(page.getPageId(), 100);
            }
            try {
                ByteArrayOutputStream bos = new ByteArrayOutputStream();
                ObjectOutput out = new ObjectOutputStream(bos);
                out.writeObject(curGraceHashBucketPage);
                page.setPageData(bos.toByteArray());
                bufferManager.unpinPage(curGraceHashBucketPage.getPageId(), true);
            } catch (IOException e) {
                e.printStackTrace();
            }
        } else {
//            System.out.println(key.toString() + ": already has an index");
            Page page = bufferManager.fetchPage(graceHashHeaderPage.getHashArr()[index]);
            try {
                ByteArrayInputStream bis = new ByteArrayInputStream(page.getPageData());
                ObjectInputStream in = new ObjectInputStream(bis);
                GraceHashBucketPage<K, V> curGraceHashBucketPage = (GraceHashBucketPage<K, V>) in.readObject();
//                System.out.println("curGraceHashBucketPage pageId: " + curGraceHashBucketPage.getPageId() + ", maxSize: " + curGraceHashBucketPage.getMaxSize());
                while (!curGraceHashBucketPage.insert(key, value)) {
//                    System.out.println(key.toString() + ": create new bucket in while");
                    page = bufferManager.newPage();
                    curGraceHashBucketPage.setNextPageId(page.getPageId());
                    curGraceHashBucketPage = new GraceHashBucketPage<>(page.getPageId(), 100);
                }
                ByteArrayOutputStream bos = new ByteArrayOutputStream();
                ObjectOutput out = new ObjectOutputStream(bos);
                out.writeObject(curGraceHashBucketPage);
                page.setPageData(bos.toByteArray());
                bufferManager.unpinPage(curGraceHashBucketPage.getPageId(), true);
            } catch (IOException | ClassNotFoundException e) {
                e.printStackTrace();
            }
        }
    }

    public V find(K key) {
        int index = hash(key);

        return null;
    }

    public void delete() {

    }

    private int hash(K key) {
        return (Integer) key % bucketSize;
    }
}
