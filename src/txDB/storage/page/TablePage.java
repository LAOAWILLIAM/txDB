package txDB.storage.page;

import java.nio.ByteBuffer;
import java.util.Arrays;

import txDB.Config;
import txDB.concurrency.LockManager;
import txDB.concurrency.Transaction;
import txDB.recovery.LogManager;
import txDB.recovery.LogRecord;
import txDB.storage.table.RecordID;
import txDB.storage.table.Tuple;
import txDB.concurrency.Transaction.TransactionState;

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

    public TablePage(Page page) {
        setPageId(page.getPageId());
        setPinCount(page.getPinCount());
        setPageData(page.getPageData());
        setDirty(page.getIsDirty());
    }

    public void initialize(int pageId, int pageSize, int prevPageId, LogManager logManager, Transaction txn) throws InterruptedException {
        if (Config.ENABLE_LOGGING) {
            // TODO: log record
//            LogRecord logRecord = new LogRecord();
//            int lsn = logManager.appendLogRecord(logRecord, false);
//            setLsn(lsn);
//            txn.setPrevLsn(lsn);
        }

        setTablePageId(pageId);
        setPrevPageId(prevPageId);
        setNextPageId(Config.INVALID_PAGE_ID);
        setFreeSpacePointer(pageSize);
        setTupleCount(0);
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
     * @param recordID
     * @return
     */
    public Tuple getTuple(RecordID recordID, Transaction txn, LockManager lockManager) throws InterruptedException {
        int tupleIndex = recordID.getTupleIndex();
        if (tupleIndex > getTupleCount()) {
            if (Config.ENABLE_LOGGING) {
                // abort this transaction
                txn.setTransactionState(TransactionState.ABORTED);
            }
            return null;
        }

        int tupleSize = getTupleSize(tupleIndex);
        if (tupleIsDeleted(tupleSize)) {
            if (Config.ENABLE_LOGGING) {
                // abort this transaction
                txn.setTransactionState(TransactionState.ABORTED);
            }
            return null;
        }

        // here we have a valid tuple and we shall get a shared lock
        if (!txn.isRecordSharedLocked(recordID)
                && !txn.isRecordExclusiveLocked(recordID)
                && !lockManager.acquireSharedLock(txn, recordID))
            return null;

        // after get the lock, we can return the tuple
        int tupleOffset = getTupleOffset(tupleIndex);
        byte[] tupleData = new byte[tupleSize];
        ByteBuffer pageBuffer = ByteBuffer.wrap(this.getPageData());
        int i;
        for (i = 0; i < tupleSize; i++) {
            tupleData[i] = pageBuffer.get(tupleOffset + i);
        }

        return new Tuple(tupleData, recordID, tupleSize, true);
    }

    /**
     * Insert a tuple
     * @param tuple
     * @param recordID
     * @param txn
     * @param lockManager
     * @param logManager
     * @return boolean
     */
    public boolean insertTuple(Tuple tuple, RecordID recordID, Transaction txn, LockManager lockManager, LogManager logManager) throws InterruptedException {
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

        recordID.setRecordId(getTablePageId(), i);
        if (i == getTupleCount()) setTupleCount(getTupleCount() + 1);

        if (Config.ENABLE_LOGGING) {
            // TODO
            assert lockManager.acquireExclusiveLock(txn, recordID);
            LogRecord logRecord = new LogRecord(txn.getPrevLsn(), txn.getTxnId(), LogRecord.LogRecordType.INSERT, recordID, tuple);
            int lsn = logManager.appendLogRecord(logRecord, false);
            txn.setPrevLsn(lsn);
            setLsn(lsn);
        }

        return true;
    }

    /**
     * Insert a tuple, improve performance of bulk insertion
     * @param tuple
     * @param txn
     * @param lockManager
     * @param logManager
     * @return RecordID
     * @throws InterruptedException
     */
    public RecordID insertTuple(Tuple tuple, Transaction txn, LockManager lockManager, LogManager logManager) throws InterruptedException {
        if (getRemainingFreeSpace() < tuple.getTupleSize() + TUPLE_POINTER_SIZE) return null;

        int i, tupleCount = getTupleCount();
        for (i = 0; i < tupleCount; i++) {
            if (getTupleSize(i) == 0) break;
        }

        if (i == getTupleCount() && getRemainingFreeSpace() < tuple.getTupleSize() + TUPLE_POINTER_SIZE) {
            return null;
        }

        setFreeSpacePointer(getFreeSpacePointer() - tuple.getTupleSize());
        setTupleData(tuple.getTupleData(), getFreeSpacePointer(), tuple.getTupleSize());

        setTupleOffset(i, getFreeSpacePointer());
        setTupleSize(i, tuple.getTupleSize());

        RecordID recordID = new RecordID(getTablePageId(), i);
        if (i == getTupleCount()) setTupleCount(getTupleCount() + 1);

        if (Config.ENABLE_LOGGING) {
            // TODO
//            assert lockManager.acquireExclusiveLock(txn, recordID);
            LogRecord logRecord = new LogRecord(txn.getPrevLsn(), txn.getTxnId(), LogRecord.LogRecordType.INSERT, recordID, tuple);
            int lsn = logManager.appendLogRecord(logRecord, false);
            txn.setPrevLsn(lsn);
            setLsn(lsn);
//            lockManager.unlock(txn, recordID);
        }

        return recordID;
    }

    /**
     * Update a tuple
     * @param newTuple
     * @param recordID
     * @param txn
     * @param lockManager
     * @param logManager
     * @return
     */
    public boolean updateTuple(Tuple newTuple, RecordID recordID, Transaction txn, LockManager lockManager, LogManager logManager) throws InterruptedException {
        // TODO
        int tupleIndex = recordID.getTupleIndex();
        if (tupleIndex >= getTupleCount()) {
            if (Config.ENABLE_LOGGING) {
                txn.setTransactionState(TransactionState.ABORTED);
            }
            return false;
        }

        int tupleSize = getTupleSize(tupleIndex);

        if (getRemainingFreeSpace() + tupleSize < newTuple.getTupleSize()) {
            return false;
        }

        int tupleOffset = getTupleOffset(tupleIndex);

        // copy old tuple data
        Tuple oldTuple = new Tuple();
        oldTuple.setRecordID(recordID);
        oldTuple.setTupleSize(tupleSize);
        oldTuple.setTupleData(Arrays.copyOfRange(getPageData(), tupleOffset, tupleOffset + tupleSize));
        oldTuple.setAllocated(true);

        if (Config.ENABLE_LOGGING) {
            LogRecord logRecord = new LogRecord(txn.getPrevLsn(), txn.getTxnId(), LogRecord.LogRecordType.UPDATE, recordID, oldTuple, newTuple);
            int lsn = logManager.appendLogRecord(logRecord, false);
            txn.setPrevLsn(lsn);
            setLsn(lsn);
        }

        int freeSpacePointer = getFreeSpacePointer();

        spaceMove(freeSpacePointer + tupleSize - newTuple.getTupleSize(), freeSpacePointer, tupleOffset - freeSpacePointer);
        setFreeSpacePointer(freeSpacePointer + tupleSize - newTuple.getTupleSize());
        spaceCpy(tupleOffset + tupleSize - newTuple.getTupleSize(), newTuple.getTupleData(), newTuple.getTupleSize());
        setTupleSize(tupleIndex, newTuple.getTupleSize());

        int i, tupleIOffset;
        for (i = 0; i < getTupleCount(); i++) {
            tupleIOffset = getTupleOffset(i);
            if (getTupleSize(i) > 0 && tupleIOffset < tupleOffset + tupleSize) {
                setTupleOffset(i, tupleIOffset + tupleSize - newTuple.getTupleSize());
            }
        }

        return true;
    }

    private void spaceCpy(int dest, byte[] data, int length) {
        ByteBuffer pageBuffer = ByteBuffer.wrap(this.getPageData());
        int i;
        for (i = 0; i < length; i++) {
            pageBuffer.put(dest + i, data[i]);
        }
    }

    private void spaceMove(int dest, int src, int length) {
        ByteBuffer pageBuffer = ByteBuffer.wrap(this.getPageData());
        int i;
        for (i = 0; i < length; i++) {
            pageBuffer.put(dest + i, pageBuffer.get(src + i));
        }
    }

    /**
     *
     * @param recordID
     * @return
     */
    public boolean markDelete(RecordID recordID, Transaction txn, LockManager lockManager, LogManager logManager) {
        // TODO
        return false;
    }

    /**
     *
     * @param recordID
     * @return
     */
    public boolean applyDelete(RecordID recordID, Transaction txn, LockManager lockManager, LogManager logManager) {
        // TODO
        return false;
    }

    /**
     *
     * @return
     */
    public int getTupleCount() {
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
//        this.setPageData(pageBuffer.array());
    }

    private void setTupleData(byte[] tupleData, int offset, int length) {
        ByteBuffer pageBuffer = ByteBuffer.wrap(this.getPageData());
        int i;
        for (i = 0; i < length; i++) {
            pageBuffer.put(offset + i, tupleData[i]);
        }
    }

    private int getRemainingFreeSpace() {
        return getFreeSpacePointer() - PAGE_HEADER_SIZE - TUPLE_POINTER_SIZE * getTupleCount();
    }

    private boolean tupleIsDeleted(int tupleSize) {
        // TODO
//        return tupleSize == 0 || (tupleSize & DELETE_MASK) == 0;
        return tupleSize == 0;
    }

    private int setTupleDeleted(int tupleSize) {
        return tupleSize | DELETE_MASK;
    }

    private int unsetTupleDeleted(int tupleSize) {
        return tupleSize & (~DELETE_MASK);
    }
}
