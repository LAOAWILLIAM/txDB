package txDB.storage.page;

import txDB.Config;

import java.io.Serializable;

public class GraceHashHeaderPage<K extends Comparable<K>, V> implements Serializable {
    private int[] hashArr;
    private int pageId;

    public GraceHashHeaderPage(int pageId, int bucketSize) {
        this.pageId = pageId;
        this.hashArr = new int[bucketSize];
        for (int i = 0; i < bucketSize; i++)
            this.hashArr[i] = Config.INVALID_PAGE_ID;
    }

    public int getPageId() {
        return pageId;
    }

    public int[] getHashArr() {
        return hashArr;
    }
}
