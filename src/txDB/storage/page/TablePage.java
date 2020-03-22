package txDB.storage.page;

import java.nio.ByteBuffer;

import txDB.Config;
import txDB.concurrency.LockManager;
import txDB.concurrency.Transaction;
import txDB.recovery.LogManager;
import txDB.storage.table.RecordID;
import txDB.storage.table.Tuple;

/**
 * Reference: This page format is designed by BusTub team, Carnegie Mellon University Database Group
 *
 * Slotted page format:
 *  ---------------------------------------------------------
 *  | HEADER | ... FREE SPACE ... | ... INSERTED TUPLES ... |
 *  ---------------------------------------------------------
 *                                ^
 *                                free space pointer
 *
 *  Header format (size in bytes):
 *  ----------------------------------------------------------------------------
 *  | PageId (4)| LSN (4)| PrevPageId (4)| NextPageId (4)| FreeSpacePointer(4) |
 *  ----------------------------------------------------------------------------
 *  ----------------------------------------------------------------
 *  | TupleCount (4) | Tuple_1 offset (4) | Tuple_1 size (4) | ... |
 *  ----------------------------------------------------------------
 *
 */
public class TablePage extends Page {

    private static final int PAGE_ID_OFFSET = 0;
    private static final int PREV_PAGE_ID_OFFSET = 8;
    private static final int NEXT_PAGE_ID_OFFSET = 12;
    private static final int FREE_SPACE_POINTER_OFFSET = 16;
    private static final int TUPLE_COUNT_OFFSET = 20;
    private static final int TUPLE_OFFSET_START_OFFSET = 24;
    private static final int TUPLE_SIZE_START_OFFSET = 28;
    private static final int PAGE_HEADER_SIZE = 24;
    private static final int TUPLE_POINTER_SIZE = 8;
    private static final int DELETE_MASK = (1 << (Integer.SIZE - 1));

    public TablePage() {
        super();
    }

    /**
     *
     * @return
     */
    public int getTablePageId() {
        ByteBuffer pageBuffer = ByteBuffer.wrap(this.getPageData());
        return pageBuffer.getInt(PAGE_ID_OFFSET);
    }

    /**
     *
     * @return
     */
    public int getPrevPageId() {
        ByteBuffer pageBuffer = ByteBuffer.wrap(this.getPageData());
        return pageBuffer.getInt(PREV_PAGE_ID_OFFSET);
    }

    /**
     *
     * @return
     */
    public int getNextPageId() {
        ByteBuffer pageBuffer = ByteBuffer.wrap(this.getPageData());
        return pageBuffer.getInt(NEXT_PAGE_ID_OFFSET);
    }

    /**
     *
     * @param pageId
     */
    public void setTablePageId(int pageId) {
        ByteBuffer pageBuffer = ByteBuffer.wrap(this.getPageData());
        pageBuffer.putInt(PAGE_ID_OFFSET, pageId);
        this.setPageData(pageBuffer.array());
    }

    /**
     *
     * @param prevPageId
     */
    public void setPrevPageId(int prevPageId) {
        ByteBuffer pageBuffer = ByteBuffer.wrap(this.getPageData());
        pageBuffer.putInt(PREV_PAGE_ID_OFFSET, prevPageId);
        this.setPageData(pageBuffer.array());
    }

    /**
     *
     * @param nextPageId
     */
    public void setNextPageId(int nextPageId) {
        ByteBuffer pageBuffer = ByteBuffer.wrap(this.getPageData());
        pageBuffer.putInt(NEXT_PAGE_ID_OFFSET, nextPageId);
        this.setPageData(pageBuffer.array());
    }

    /**
     *
     * @param rid
     * @return
     */
    public Tuple getTuple(RecordID rid, Transaction txn, LockManager lockManager) {
        int tupleIndex = rid.getTupleIndex();
        if (tupleIndex > getTupleCount()) {
            if (Config.ENABLE_LOGGING) {
                // abort this transaction
                // TODO
            }
            return null;
        }

        int tupleSize = getTupleSize(tupleIndex);
        if (tupleIsDeleted(tupleSize)) {
            if (Config.ENABLE_LOGGING) {
                // abort this transaction
                // TODO
            }
            return null;
        }

        // here we have a valid tuple and we shall get a shared lock
        // TODO

        // after get the lock, we can return the tuple
        int tupleOffset = getTupleOffset(tupleIndex);
        byte[] tupleData = new byte[tupleSize];
        ByteBuffer pageBuffer = ByteBuffer.wrap(this.getPageData());
        pageBuffer.get(tupleData, tupleOffset, tupleSize);

        Tuple newTuple = new Tuple();
        newTuple.setTupleSize(tupleSize);
        newTuple.setRecordID(rid);
        newTuple.setAllocated(true);
        newTuple.setTupleData(tupleData);

        return newTuple;
    }

    /**
     *
     * @param tuple
     * @param rid
     * @param txn
     * @param lockManager
     * @param logManager
     * @return
     */
    public boolean insertTuple(Tuple tuple, RecordID rid, Transaction txn, LockManager lockManager, LogManager logManager) {
        if (getRemainingFreeSpace() < tuple.getTupleSize() + TUPLE_POINTER_SIZE) return false;

        int i, tupleCount = getTupleCount();
        for (i = 0; i < tupleCount; i++) {
            if (getTupleSize(i) == 0) break;
        }

        if (i == getTupleCount() && getRemainingFreeSpace() < tuple.getTupleSize() + TUPLE_POINTER_SIZE) {
            return false;
        }

        setFreeSpacePointer(getFreeSpacePointer() - tuple.getTupleSize());
        setTupleData(tuple.getTupleData(), getFreeSpacePointer(), tuple.getTupleSize());

        setTupleOffset(i, getFreeSpacePointer());
        setTupleSize(i, tuple.getTupleSize());

        rid.setRecordId(getTablePageId(), i);
        if (i == getTupleCount()) setTupleCount(getTupleCount() + 1);

        if (Config.ENABLE_LOGGING) {
            // TODO
        }

        return true;
    }

    /**
     *
     * @param rid
     * @return
     */
    public boolean updateTuple(RecordID rid, Transaction txn, LockManager lockManager, LogManager logManager) {
        // TODO
        return false;
    }

    /**
     *
     * @param rid
     * @return
     */
    public boolean markDelete(RecordID rid, Transaction txn, LockManager lockManager, LogManager logManager) {
        // TODO
        return false;
    }

    /**
     *
     * @param rid
     * @return
     */
    public boolean applyDelete(RecordID rid, Transaction txn, LockManager lockManager, LogManager logManager) {
        // TODO
        return false;
    }

    /**
     *
     * @return
     */
    private int getTupleCount() {
        ByteBuffer pageBuffer = ByteBuffer.wrap(this.getPageData());
        return pageBuffer.getInt(TUPLE_COUNT_OFFSET);
    }

    /**
     *
     * @param tupleCount
     */
    public void setTupleCount(int tupleCount) {
        ByteBuffer pageBuffer = ByteBuffer.wrap(this.getPageData());
        pageBuffer.putInt(TUPLE_COUNT_OFFSET, tupleCount);
        this.setPageData(pageBuffer.array());
    }

    /**
     *
     * @param tupleIndex
     * @return
     */
    private int getTupleOffset(int tupleIndex) {
        ByteBuffer pageBuffer = ByteBuffer.wrap(this.getPageData());
        return pageBuffer.getInt(TUPLE_OFFSET_START_OFFSET + tupleIndex * 8);
    }

    /**
     *
     */
    private void setTupleOffset(int tupleIndex, int offset) {
        ByteBuffer pageBuffer = ByteBuffer.wrap(this.getPageData());
        pageBuffer.putInt(TUPLE_OFFSET_START_OFFSET + tupleIndex * TUPLE_POINTER_SIZE, offset);
        this.setPageData(pageBuffer.array());
    }

    /**
     *
     * @param tupleIndex
     * @return
     */
    private int getTupleSize(int tupleIndex) {
        ByteBuffer pageBuffer = ByteBuffer.wrap(this.getPageData());
        return pageBuffer.getInt(TUPLE_SIZE_START_OFFSET + tupleIndex * 8);
    }

    /**
     *
     */
    private void setTupleSize(int tupleIndex, int tupleSize) {
        ByteBuffer pageBuffer = ByteBuffer.wrap(this.getPageData());
        pageBuffer.putInt(TUPLE_SIZE_START_OFFSET + tupleIndex * TUPLE_POINTER_SIZE, tupleSize);
        this.setPageData(pageBuffer.array());
    }

    /**
     *
     * @return
     */
    private int getFreeSpacePointer() {
        ByteBuffer pageBuffer = ByteBuffer.wrap(this.getPageData());
        return pageBuffer.getInt(FREE_SPACE_POINTER_OFFSET);
    }

    /**
     *
     * @param freeSpacePointer
     */
    public void setFreeSpacePointer(int freeSpacePointer) {
        ByteBuffer pageBuffer = ByteBuffer.wrap(this.getPageData());
        pageBuffer.putInt(FREE_SPACE_POINTER_OFFSET, freeSpacePointer);
        this.setPageData(pageBuffer.array());
    }

    private void setTupleData(byte[] tupleData, int offset, int length) {
        ByteBuffer pageBuffer = ByteBuffer.wrap(this.getPageData());
        pageBuffer.put(tupleData, offset, length);
        this.setPageData(pageBuffer.array());
    }

    private int getRemainingFreeSpace() {
        return getFreeSpacePointer() - PAGE_HEADER_SIZE - TUPLE_POINTER_SIZE * getTupleCount();
    }

    private boolean tupleIsDeleted(int tupleSize) {
        return tupleSize == 0 || (tupleSize & DELETE_MASK) == 0;
    }

    private int setTupleDeleted(int tupleSize) {
        return tupleSize | DELETE_MASK;
    }

    private int unsetTupleDeleted(int tupleSize) {
        return tupleSize & (~DELETE_MASK);
    }
}
