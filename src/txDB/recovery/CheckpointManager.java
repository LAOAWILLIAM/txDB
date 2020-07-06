package txDB.recovery;

import txDB.Config;
import txDB.buffer.BufferManager;
import txDB.concurrency.TransactionManager;
import txDB.storage.page.MetaDataPage;
import txDB.storage.page.Page;

import java.io.*;
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
    private LogManager logManager;
    private TransactionManager transactionManager;
    private ExecutorService checkpointService;
    private boolean ENABLE_CHECKPOINT;

    public CheckpointManager(BufferManager bufferManager, TransactionManager transactionManager, LogManager logManager) {
        this.bufferManager = bufferManager;
        this.transactionManager = transactionManager;
        this.logManager = logManager;
        checkpointService = Executors.newSingleThreadExecutor();
    }

    private class checkpointThread implements Runnable {
        @Override
        public void run() {
            System.out.println("checkpoint thread is on");
            while (ENABLE_CHECKPOINT && !Thread.interrupted()) {
                synchronized (this) {
                    try {
                        TimeUnit.SECONDS.sleep(5);
                        begin();
                        end();
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            }
            System.out.println("checkpoint thread is off");
        }
    }

    public void startCheckpointService() {
        if (!Config.ENABLE_LOGGING) return;
        ENABLE_CHECKPOINT = true;
        checkpointService.submit(new checkpointThread());
    }

    public void closeCheckpointService() {
        // TODO: sometimes it would wait infinitely to terminate, maybe deadlock in log manager
        ENABLE_CHECKPOINT = false;
        checkpointService.shutdown();
        try {
            while (!checkpointService.awaitTermination(Config.LOGGING_TIMEOUT, TimeUnit.MILLISECONDS)) {
                System.out.println("waiting for checkpoint service to terminate");
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        System.out.println("checkpoint service is terminated");
    }

    public void begin() {
        synchronized (this) {
            // TODO: stop updates when full checkpoint is enabled
//            System.out.println("start appending begin record");
            LogRecord logRecord = new LogRecord(logManager.getNextLsn(), LogRecord.LogRecordType.CBEGIN);
            int logFileLength = logManager.appendLogRecord(logRecord, true, true);

            System.out.println("log length: " + logFileLength);

            Page page = bufferManager.fetchPage(0);
            ByteArrayInputStream bis = new ByteArrayInputStream(page.getPageData());
            try {
                ObjectInputStream in = new ObjectInputStream(bis);
                MetaDataPage metaDataPage = (MetaDataPage) in.readObject();
                metaDataPage.setLastCheckpointOffset(logFileLength);

                ByteArrayOutputStream bos = new ByteArrayOutputStream();
                ObjectOutput out = new ObjectOutputStream(bos);
                out.writeObject(metaDataPage);
                page.setPageData(bos.toByteArray());
                bufferManager.unpinPage(page.getPageId(), true);
            } catch (IOException | ClassNotFoundException e) {
                e.printStackTrace();
            }

//            System.out.println("finish appending begin record");
        }
    }

    public void end() {
        synchronized (this) {
//            System.out.println("start appending end record");
//            System.out.println("start new a log record");

            LogRecord logRecord = new LogRecord(
                    logManager.getNextLsn(), LogRecord.LogRecordType.CEND,
                    new ActiveTxnsAndDirtyPages(transactionManager.getActiveTxnMap(), bufferManager.flushAllDirtyPages(Config.ENABLE_FUZZY_CHECKPOINT)));
            logManager.appendLogRecord(logRecord, true, true);

            // TODO: resume updates when full checkpoint is enabled

//            System.out.println("finish appending end record");
        }
    }
}
