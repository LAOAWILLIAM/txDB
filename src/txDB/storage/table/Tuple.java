package txDB.storage.table;

/**
 * Tuple format:
 * ---------------------------------------------------------------------
 * | FIXED-SIZE or VARIED-SIZED OFFSET | PAYLOAD OF VARIED-SIZED FIELD |
 * ---------------------------------------------------------------------
 */
public class Tuple {
    private byte[] tupleData;
    private RecordID recordID;
    private int tupleSize;
    private boolean isAllocated;

    public Tuple() {}

    public Tuple(byte[] tupleData, RecordID recordID, int tupleSize, boolean isAllocated) {
        this.tupleData = tupleData;
        this.recordID = recordID;
        this.tupleSize = tupleSize;
        this.isAllocated = isAllocated;
    }

    public byte[] getTupleData() {
        return this.tupleData;
    }

    public void setTupleData(byte[] tupleData) {
        this.tupleData = tupleData;
    }

    public RecordID getRecordID() {
        return this.recordID;
    }

    public void setRecordID(RecordID recordID) {
        this.recordID = recordID;
    }

    public int getTupleSize() {
        return this.tupleSize;
    }

    public void setTupleSize(int tupleSize) {
        this.tupleSize = tupleSize;
    }

    public boolean isAllocated() {
        return this.isAllocated;
    }

    public void setAllocated(boolean allocated) {
        this.isAllocated = allocated;
    }

}
