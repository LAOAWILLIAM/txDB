package txDB.storage.page;

import txDB.Config;
import txDB.storage.index.BPlusTreeIndex;

import java.io.Serializable;
import java.util.ArrayList;

public class BPlusTreePageNode<K extends Comparable<K>, V> implements Serializable {
    public enum IndexPageType{LEAFPAGE, INNERPAGE}
    // TODO
    private int pageId;
    private int parentPageId;
    private IndexPageType indexPageType;
    protected ArrayList<K> keys;
    private int maxSize;
    private int size;

    public BPlusTreePageNode() {
        this.keys = new ArrayList<>();
    }

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

    public void setSize(int size) {
        this.size = size;
    }

    public ArrayList<K> getKeys() {
        return this.keys;
    }

    public boolean isOverSized() {
        return keys.size() >= maxSize;
    }

    public boolean isUnderSized() {
        return keys.size() < Math.round(maxSize / 2.0 - 1);
    }

    public boolean isLeafPageNode() {
        return indexPageType == IndexPageType.LEAFPAGE;
    }

    public boolean isRootPageNode() {
        return parentPageId == Config.INVALID_PAGE_ID;
    }
}
