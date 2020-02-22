package txDB.storage.page;

import txDB.Config;
import txDB.storage.index.BPlusTreeIndex;

import java.io.Serializable;
import java.util.ArrayList;

public class BPlusTreePage<K extends Comparable<K>, V> implements Serializable {
    public enum IndexPageType{LEAFPAGE, INNERPAGE}
    // TODO
    private int pageId;
    private int parentPageId;
    private IndexPageType indexPageType;
    private ArrayList<K> keys;
    private int maxSize;

    public IndexPageType getIndexPageType() {
        return this.indexPageType;
    }

    public void setIndexPageType(IndexPageType indexPageType) {
        this.indexPageType = indexPageType;
    }

    public int getParentPageId() {
        return this.parentPageId;
    }

    public void setParentPageId(int parentPageId) {
        this.parentPageId = parentPageId;
    }

    public int getPageId() {
        return this.pageId;
    }

    public void setPageId(int pageId) {
        this.pageId = pageId;
    }

    public int getMaxSize() {
        return this.maxSize;
    }

    public void setMaxSize(int maxSize) {
        this.maxSize = maxSize;
    }

    public boolean isOverSized() {
        return keys.size() >= maxSize;
    }

    public boolean isUnderSized() {
        return keys.size() < Math.round(maxSize / 2.0 - 1);
    }

    public boolean isLeafPage() {
        return indexPageType == IndexPageType.LEAFPAGE;
    }

    public boolean isRootPage() {
        return parentPageId == Config.INVALID_PAGE_ID;
    }
}
