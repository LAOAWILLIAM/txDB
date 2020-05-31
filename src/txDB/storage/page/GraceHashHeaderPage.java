package txDB.storage.page;

import java.io.Serializable;

public class GraceHashHeaderPage<K extends Comparable<K>, V> implements Serializable {
    private int[] hashArr;
    private int pageId;

    public GraceHashHeaderPage(int pageId, int bucketSize) {
        this.pageId = pageId;
        this.hashArr = new int[bucketSize];
    }

    public int getPageId() {
        return pageId;
    }

    public int[] getHashArr() {
        return hashArr;
    }
}
