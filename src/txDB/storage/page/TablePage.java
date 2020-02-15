package txDB.storage.page;

import java.nio.ByteBuffer;

/**
 * Note: This page format is designed by BusTub team, Carnegie Mellon University Database Group
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

    public TablePage() {
        super();
    }

    public int getTablePageId() {
        ByteBuffer pageBuffer = ByteBuffer.wrap(this.getPageData());
        int pageId = pageBuffer.getInt(PAGE_ID_OFFSET);
        return pageId;
    }

    public int getPrevPageId() {
        ByteBuffer pageBuffer = ByteBuffer.wrap(this.getPageData());
        return pageBuffer.getInt(PREV_PAGE_ID_OFFSET);
    }

    public int getNextPageId() {
        ByteBuffer pageBuffer = ByteBuffer.wrap(this.getPageData());
        return pageBuffer.getInt(NEXT_PAGE_ID_OFFSET);
    }

    public void setTablePageId(int pageId) {
        ByteBuffer pageBuffer = ByteBuffer.wrap(this.getPageData());
        pageBuffer.putInt(PAGE_ID_OFFSET, pageId);
        this.setPageData(pageBuffer.array());
    }

    public void setPrevPageId(int prevPageId) {
        ByteBuffer pageBuffer = ByteBuffer.wrap(this.getPageData());
        pageBuffer.putInt(PREV_PAGE_ID_OFFSET, prevPageId);
        this.setPageData(pageBuffer.array());
    }

    public void setNextPageId(int nextPageId) {
        ByteBuffer pageBuffer = ByteBuffer.wrap(this.getPageData());
        pageBuffer.putInt(NEXT_PAGE_ID_OFFSET, nextPageId);
        this.setPageData(pageBuffer.array());
    }
}
