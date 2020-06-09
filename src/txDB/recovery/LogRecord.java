package txDB.recovery;

import txDB.Config;

import java.io.Serializable;

public class LogRecord implements Serializable {
    // TODO
    public enum LogRecordType {INSERT, UPDATE, BEGIN, COMMIT, ABORT}
    private int logSize = 0;
    private LogRecordType logRecordType;
    private int txnId = Config.INVALID_TXN_ID;
    private int lsn = Config.INVALID_LSN;

}
