package txDB.storage.page;

import java.nio.ByteBuffer;

import txDB.storage.table.RecordID;

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
    public boolean getTuple(RecordID rid) {
        // TODO
        return false;
    }

    /**
     *
     * @param rid
     * @return
     */
    public boolean insertTuple(RecordID rid) {
        // TODO
        return false;
    }

    /**
     *
     * @param rid
     * @return
     */
    public boolean updateTuple(RecordID rid) {
        // TODO
        return false;
    }

    /**
     *
     * @param rid
     * @return
     */
    public boolean markDelete(RecordID rid) {
        // TODO
        return false;
    }

    /**
     *
     * @param rid
     * @return
     */
    public boolean applyDelete(RecordID rid) {
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
    private void setTupleCount(int tupleCount) {
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
    private void setTupleOffset() {
        // TODO
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
    private void setTupleSize() {
        // TODO
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
    private void setFreeSpacePointer(int freeSpacePointer) {
        ByteBuffer pageBuffer = ByteBuffer.wrap(this.getPageData());
        pageBuffer.putInt(FREE_SPACE_POINTER_OFFSET, freeSpacePointer);
        this.setPageData(pageBuffer.array());
    }
}
