package txDB.storage.table;

import java.io.Serializable;

public class RecordID implements Serializable {
    private int pageId;
    private int tupleIndex;

    public RecordID(int pageId, int tupleIndex) {
        this.pageId = pageId;
        this.tupleIndex = tupleIndex;
    }

    public int getPageId() {
        return this.pageId;
    }

    public int getTupleIndex() {
        return this.tupleIndex;
    }

    public void setRecordId(int pageId, int tupleIndex) {
        this.pageId = pageId;
        this.tupleIndex = tupleIndex;
    }
}
