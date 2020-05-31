package txDB.execution.hash;

import txDB.buffer.BufferManager;
import txDB.storage.page.GraceHashBucketPage;
import txDB.storage.page.GraceHashHeaderPage;
import txDB.storage.page.Page;

import java.io.*;

public class GraceHash<K extends Comparable<K>, V> {
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
        graceHashHeaderPage = new GraceHashHeaderPage<K, V>(page.getPageId(), bucketSize);
        try {
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            ObjectOutput out = new ObjectOutputStream(bos);
            out.writeObject(graceHashHeaderPage);
            page.setPageData(bos.toByteArray());
            bufferManager.unpinPage(graceHashHeaderPage.getPageId(), true);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @SuppressWarnings("unchecked")
    public void insert(K key, V value) {
        int index = hash(key);
        if (graceHashHeaderPage.getHashArr()[index] == 0) {
            Page page = bufferManager.newPage();
            graceHashHeaderPage.getHashArr()[index] = page.getPageId();
            GraceHashBucketPage<K, V> curGraceHashBucketPage = new GraceHashBucketPage<>(page.getPageId(), 300);
            while (!curGraceHashBucketPage.insert(key, value)) {
                page = bufferManager.newPage();
                curGraceHashBucketPage.setNextPageId(page.getPageId());
                curGraceHashBucketPage = new GraceHashBucketPage<>(page.getPageId(), 300);
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
            Page page = bufferManager.fetchPage(graceHashHeaderPage.getHashArr()[index]);
            try {
                ByteArrayInputStream bis = new ByteArrayInputStream(page.getPageData());
                ObjectInputStream in = new ObjectInputStream(bis);
                GraceHashBucketPage<K, V> curGraceHashBucketPage = (GraceHashBucketPage<K, V>) in.readObject();
                while (!curGraceHashBucketPage.insert(key, value)) {
                    page = bufferManager.newPage();
                    curGraceHashBucketPage.setNextPageId(page.getPageId());
                    curGraceHashBucketPage = new GraceHashBucketPage<>(page.getPageId(), 300);
                }
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
        return -1;
    }
}
