package txDB.recovery;

import txDB.Config;
import txDB.buffer.BufferManager;
import txDB.concurrency.TransactionManager;
import txDB.storage.disk.DiskManager;

import java.util.HashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * I use ARIES's fuzzy checkpoint scheme here,
 * which only records information about dirty pages,
 * and does not require dirty pages to be written out at checkpoint time,
 * because of that, there is no need to temporarily stop all updates by transactions.
 */
public class CheckpointManager {
    private BufferManager bufferManager;
    private DiskManager diskManager;
    private LogManager logManager;
    private TransactionManager transactionManager;
    private ExecutorService checkpointService;

    public CheckpointManager(BufferManager bufferManager, DiskManager diskManager, TransactionManager transactionManager, LogManager logManager) {
        this.bufferManager = bufferManager;
        this.diskManager = diskManager;
        this.transactionManager = transactionManager;
        this.logManager = logManager;
        checkpointService = Executors.newSingleThreadExecutor();
    }

    private class checkpointThread implements Runnable {
        @Override
        public void run() {
            while (Config.ENABLE_LOGGING && !Thread.interrupted()) {
                try {
                    TimeUnit.SECONDS.sleep(5);
                    begin();
                    end();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    public void startCheckpointService() {
        if (Config.ENABLE_LOGGING) return;
        checkpointService.submit(new checkpointThread());
    }

    public void begin() {
        // TODO: stop updates when full checkpoint is enabled

        LogRecord logRecord = new LogRecord(logManager.getNextLsn(), LogRecord.LogRecordType.CBEGIN);
        logManager.appendLogRecord(logRecord, true);
    }

    public void end() {
        LogRecord logRecord = new LogRecord(
                logManager.getNextLsn(), LogRecord.LogRecordType.CEND,
                transactionManager.getActiveTxnMap(), bufferManager.flushAllDirtyPages(Config.ENABLE_FUZZY_CHECKPOINT));
        logManager.appendLogRecord(logRecord, true);

        // TODO: resume updates when full checkpoint is enabled
    }
}
