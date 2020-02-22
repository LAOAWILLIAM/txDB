package txDB.storage.page;

import txDB.Config;

import java.io.Serializable;
//import java.lang.instrument.Instrumentation;

public class BPlusTreeLeafPage<K extends Comparable<K>, V> extends BPlusTreePage implements Serializable {
    // TODO
    private int nextPageId;
//    private static Instrumentation instrumentation;

    public BPlusTreeLeafPage(int pageId, int parentPageId) {
        setPageId(pageId);
        setIndexPageType(IndexPageType.LEAFPAGE);
        setParentPageId(parentPageId);
        setNextPageId(Config.INVALID_PAGE_ID);
    }

    public int getNextPageId() {
        return this.nextPageId;
    }

    public void setNextPageId(int nextPageId) {
        this.nextPageId = nextPageId;
    }
}
