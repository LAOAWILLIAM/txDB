package txDB.recovery;

import txDB.Config;
import txDB.concurrency.Transaction;
import txDB.storage.page.Page;
import txDB.storage.table.RecordID;
import txDB.storage.table.Tuple;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.HashMap;

public class LogRecord implements Serializable {
    /**
     * TODO: should consider if logRecordBuffer does not have enough space
     *  one solution is to allocate enough space at the first
     */
    public enum LogRecordType {INVALID, INSERT, UPDATE, BEGIN, COMMIT, ABORT, CLR, END, CBEGIN, CEND}
    private int logSize = 0;
    private LogRecordType logRecordType = LogRecordType.INVALID;
    private int txnId = Config.INVALID_TXN_ID;
    private int lsn = Config.INVALID_LSN;
    private int prevLsn = Config.INVALID_LSN;
    private ByteBuffer logRecordBuffer = ByteBuffer.allocate(256);

    private RecordID recordID;

    // For insert
    private Tuple tuple;

    // For update and CLR
    private Tuple oldTuple;
    private Tuple newTuple;

    // For CLR
    private int undoNext = Config.INVALID_LSN;

    /**
     * For transaction begin/commit/abort
     * @param txnId
     * @param logRecordType
     */
    public LogRecord(int prevLsn, int txnId, LogRecordType logRecordType) {
        this.prevLsn = prevLsn;
        this.txnId = txnId;
        this.logRecordType = logRecordType;
        logSize = 24;

        logRecordBuffer.putInt(0, logSize);
        logRecordBuffer.putInt(8, prevLsn);
        logRecordBuffer.putInt(12, txnId);
        int i = 0;
        for (byte v: logRecordType.name().getBytes()) {
            logRecordBuffer.put(16 + i, v);
            i++;
        }
    }

    /**
     * For insert
     * @param txnId
     * @param logRecordType
     * @param recordID
     * @param tuple
     */
    public LogRecord(int prevLsn, int txnId, LogRecordType logRecordType, RecordID recordID, Tuple tuple) {
        this.prevLsn = prevLsn;
        this.txnId = txnId;
        this.logRecordType = logRecordType;
        this.recordID = recordID;
        this.tuple = tuple;

        logRecordBuffer.putInt(8, prevLsn);
        logRecordBuffer.putInt(12, txnId);
        int i = 0;
        for (byte v: logRecordType.name().getBytes()) {
            logRecordBuffer.put(16 + i, v);
            i++;
        }
        logRecordBuffer.putInt(24, recordID.getPageId());
        logRecordBuffer.putInt(28, recordID.getTupleIndex());
        i = 0;
        // TODO: length of tuple data should be checked
        for (byte v: tuple.getTupleData()) {
            logRecordBuffer.put(32 + i, v);
            i++;
        }
        logSize = 32 + i;
//        System.out.println(recordID.getPageId() + ", " + recordID.getTupleIndex() + ": " + logSize);
        logRecordBuffer.putInt(0, logSize);
    }

    /**
     * For update
     * @param txnId
     * @param logRecordType
     * @param recordID
     */
    public LogRecord(int prevLsn, int txnId, LogRecordType logRecordType, RecordID recordID, Tuple oldTuple, Tuple newTuple) {
        this.prevLsn = prevLsn;
        this.txnId = txnId;
        this.logRecordType = logRecordType;
        this.recordID = recordID;
        this.oldTuple = oldTuple;
        this.newTuple = newTuple;

        logRecordBuffer.putInt(8, prevLsn);
        logRecordBuffer.putInt(12, txnId);
        int i = 0;
        for (byte v: logRecordType.name().getBytes()) {
            logRecordBuffer.put(16 + i, v);
            i++;
        }
        logRecordBuffer.putInt(24, recordID.getPageId());
        logRecordBuffer.putInt(28, recordID.getTupleIndex());
        logRecordBuffer.putInt(32, oldTuple.getTupleSize());
        for (i = 0; i < oldTuple.getTupleSize(); i++) {
            logRecordBuffer.put(36 + i, oldTuple.getTupleData()[i]);
        }
        logRecordBuffer.putInt(36 + oldTuple.getTupleSize(), newTuple.getTupleSize());
        for (i = 0; i < newTuple.getTupleSize(); i++) {
            logRecordBuffer.put(40 + oldTuple.getTupleSize() + i, newTuple.getTupleData()[i]);
        }
        logSize = 40 + oldTuple.getTupleSize() + i;
//        System.out.println(recordID.getPageId() + ", " + recordID.getTupleIndex() + ": " + logSize);
        logRecordBuffer.putInt(0, logSize);
    }

    /**
     * For abort CLR
     * @param prevLsn
     * @param txnId
     * @param logRecordType
     * @param recordID
     * @param oldTuple
     * @param newTuple
     * @param undoNext
     */
    public LogRecord(int prevLsn, int txnId, LogRecordType logRecordType, RecordID recordID, Tuple oldTuple, Tuple newTuple, int undoNext) {
        this.prevLsn = prevLsn;
        this.txnId = txnId;
        this.logRecordType = logRecordType;
        this.recordID = recordID;
        this.oldTuple = oldTuple;
        this.newTuple = newTuple;
        this.undoNext = undoNext;

        logRecordBuffer.putInt(8, prevLsn);
        logRecordBuffer.putInt(12, txnId);
        int i = 0;
        for (byte v: logRecordType.name().getBytes()) {
            logRecordBuffer.put(16 + i, v);
            i++;
        }
        logRecordBuffer.putInt(24, recordID.getPageId());
        logRecordBuffer.putInt(28, recordID.getTupleIndex());
        logRecordBuffer.putInt(32, oldTuple.getTupleSize());
        for (i = 0; i < oldTuple.getTupleSize(); i++) {
            logRecordBuffer.put(36 + i, oldTuple.getTupleData()[i]);
        }
        logRecordBuffer.putInt(36 + oldTuple.getTupleSize(), newTuple.getTupleSize());
        for (i = 0; i < newTuple.getTupleSize(); i++) {
            logRecordBuffer.put(40 + oldTuple.getTupleSize() + i, newTuple.getTupleData()[i]);
        }
        logRecordBuffer.putInt(40 + oldTuple.getTupleSize() + i, undoNext);
        logSize = 44 + oldTuple.getTupleSize() + i;
//        System.out.println(recordID.getPageId() + ", " + recordID.getTupleIndex() + ": " + logSize);
        logRecordBuffer.putInt(0, logSize);
    }

    /**
     * For checkpoint begin
     * @param prevLsn
     * @param logRecordType
     */
    public LogRecord(int prevLsn, LogRecordType logRecordType) {
        this.prevLsn = prevLsn;
        this.logRecordType = logRecordType;
        logSize = 20;

        logRecordBuffer.putInt(0, logSize);
        logRecordBuffer.putInt(8, prevLsn);
        int i = 0;
        for (byte v: logRecordType.name().getBytes()) {
            logRecordBuffer.put(12 + i, v);
            i++;
        }
    }

    /**
     * For checkpoint end
     * @param prevLsn
     * @param logRecordType
     * @param activeTxnMap
     */
    public LogRecord(int prevLsn, LogRecordType logRecordType, HashMap<Integer, Transaction> activeTxnMap, HashMap<Integer, Page> dirtyPageMap) {
        this.prevLsn = prevLsn;
        this.logRecordType = logRecordType;

        logRecordBuffer.putInt(8, prevLsn);
        int i = 0;
        for (byte v: logRecordType.name().getBytes()) {
            logRecordBuffer.put(12 + i, v);
            i++;
        }

        try {
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            ObjectOutput out = new ObjectOutputStream(bos);
            out.writeObject(activeTxnMap);
            byte[] activeTxnMapBytes = bos.toByteArray();

            logRecordBuffer.putInt(20, activeTxnMapBytes.length);
            for (i = 0; i < activeTxnMapBytes.length; i++) {
                logRecordBuffer.put(24 + i, activeTxnMapBytes[i]);
            }

            bos.reset();
            out = new ObjectOutputStream(bos);
            out.writeObject(dirtyPageMap);
            byte[] dirtyPageMapBytes = bos.toByteArray();

            logRecordBuffer.putInt(24 + i, dirtyPageMapBytes.length);
            int j;
            for (j = 0; j < dirtyPageMapBytes.length; j++) {
                logRecordBuffer.put(28 + i + j, dirtyPageMapBytes[j]);
            }

            logSize = 28 + i + j;
            logRecordBuffer.putInt(0, logSize);

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public LogRecord() {}

    public int getTxnId() {
        return txnId;
    }

    public int getPrevLsn() {
        return prevLsn;
    }

    public void setPrevLsn(int prevLsn) {
        this.prevLsn = prevLsn;
    }

    public int getLogSize() {
        return logSize;
    }

    public LogRecordType getLogRecordType() {
        return logRecordType;
    }

    public ByteBuffer getLogRecordBuffer() {
        return logRecordBuffer;
    }

    public int getLsn() {
        return lsn;
    }

    public void setLsn(int lsn) {
        this.lsn = lsn;
        logRecordBuffer.putInt(4, lsn);
        logRecordBuffer.position(logSize);
    }

    public RecordID getRecordID() {
        return recordID;
    }

    public Tuple getOldTuple() {
        return oldTuple;
    }

    public Tuple getNewTuple() {
        return newTuple;
    }

    // For test
    @Override
    public String toString() {
        return "Log:[size: " + logRecordBuffer.getInt(0)
                + ", lsn: " + logRecordBuffer.getInt(4)
                + ", prevLsn: " + logRecordBuffer.getInt(8)
                + ", txnId: " + logRecordBuffer.getInt(12)
                + ", type: " + logRecordBuffer.get(16)
                + ", pos: " + logRecordBuffer.position() + "]";
    }

//    @Override
//    public String toString() {
//        return "LogRecord{" +
//                "logSize=" + logSize +
//                ", logRecordType=" + logRecordType +
//                ", txnId=" + txnId +
//                ", lsn=" + lsn +
//                ", prevLsn=" + prevLsn +
//                ", logRecordBuffer=" + logRecordBuffer +
//                ", recordID=" + recordID +
//                ", tuple=" + tuple +
//                ", oldTuple=" + oldTuple +
//                ", newTuple=" + newTuple +
//                ", undoNext=" + undoNext +
//                '}';
//    }
}
