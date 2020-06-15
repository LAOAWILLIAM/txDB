package txDB.recovery;

import txDB.buffer.BufferManager;
import txDB.storage.disk.DiskManager;

/**
 * This is a recovery manager after system crash
 * One of the hardest parts in this project !!!
 */
public class LogRecovery {
    // TODO
    private DiskManager diskManager;
    private BufferManager bufferManager;

    public LogRecovery(DiskManager diskManager, BufferManager bufferManager) {
        this.diskManager = diskManager;
        this.bufferManager = bufferManager;
    }
}
