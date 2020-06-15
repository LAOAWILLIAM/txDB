package txDB.recovery;

import txDB.Config;
import txDB.storage.table.RecordID;
import txDB.storage.table.Tuple;

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.Arrays;

public class LogRecord implements Serializable {
    // TODO
    public enum LogRecordType {INVALID, INSERT, UPDATE, BEGIN, COMMIT, ABORT}
    private int logSize = 0;
    private LogRecordType logRecordType = LogRecordType.INVALID;
    private int txnId = Config.INVALID_TXN_ID;
    private int lsn = Config.INVALID_LSN;
    private int prevLsn = Config.INVALID_LSN;
    private ByteBuffer logRecordBuffer = ByteBuffer.allocate(128);

    private RecordID recordID;
    private Tuple tuple;

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
    public LogRecord(int prevLsn, int txnId, LogRecordType logRecordType, RecordID recordID) {
        this.prevLsn = prevLsn;
        this.txnId = txnId;
        this.logRecordType = logRecordType;
        this.recordID = recordID;

        logRecordBuffer.putInt(8, prevLsn);
        logRecordBuffer.putInt(12, txnId);
        int i = 0;
        for (byte v: logRecordType.name().getBytes()) {
            logRecordBuffer.put(16 + i, v);
            i++;
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

    // For test
    public String toString() {
        return "Log:[size: " + logRecordBuffer.getInt(0)
                + ", lsn: " + logRecordBuffer.getInt(4)
                + ", prevLsn: " + logRecordBuffer.getInt(8)
                + ", txnId: " + logRecordBuffer.getInt(12)
                + ", pos: " + logRecordBuffer.position() + "]";
    }
}
