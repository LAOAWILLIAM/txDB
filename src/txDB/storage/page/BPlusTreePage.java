package txDB.storage.page;

import java.io.Serializable;

public class BPlusTreePage implements Serializable {
    public enum IndexPageType{LEAFPAGE, INNERPAGE}
    // TODO
    private int pageId;
    private int parentPageId;
    private boolean isRoofPage;
    private boolean isLeafPage;
    private IndexPageType indexPageType;

    public IndexPageType getIndexPageType() {
        return this.indexPageType;
    }

    public void setIndexPageType(IndexPageType indexPageType) {
        this.indexPageType = indexPageType;
    }

    public int getParentPageId() {
        return this.parentPageId;
    }

    public int getPageId() {
        return this.pageId;
    }

    public void setPageId(int pageId) {
        this.pageId = pageId;
    }
}
