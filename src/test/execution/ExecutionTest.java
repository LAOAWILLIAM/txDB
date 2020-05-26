package test.execution;

import txDB.buffer.BufferManager;
import txDB.concurrency.LockManager;
import txDB.concurrency.Transaction;
import txDB.concurrency.TransactionManager;
import txDB.execution.executors.SeqScanExecutor;
import txDB.execution.plans.SeqScanPlan;
import txDB.storage.disk.DiskManager;
import txDB.storage.table.Column;
import txDB.storage.table.Scheme;
import txDB.storage.table.Tuple;
import txDB.type.Type;

import java.io.IOException;
import java.util.ArrayList;
import org.junit.Test;
import static org.junit.Assert.*;

public class ExecutionTest {
    // TODO
    String dbName = "test";
    DiskManager diskManager = new DiskManager();
    TransactionManager transactionManager = new TransactionManager(null, null);

    public ExecutionTest() throws IOException {
//        diskManager.dropFile(dbName);
        diskManager.createFile(dbName);
        diskManager.useFile(dbName);
    }

    @Test
    public void seqScanTest() throws InterruptedException {
        int bufferSize = 10;
        BufferManager bufferManager = new BufferManager(bufferSize, diskManager);
        LockManager lockManager = new LockManager(LockManager.twoPhaseLockType.REGULAR, LockManager.deadlockType.DETECTION);
        Transaction txn0 = transactionManager.begin();

        ArrayList<Column> columns = new ArrayList<>();
        Column col0 = new Column("col0", Type.ColumnValueType.INTEGER, 4, 0);
        Column col1 = new Column("col1", Type.ColumnValueType.INTEGER, 4, 0);
        Column col2 = new Column("col2", Type.ColumnValueType.INTEGER, 4, 0);
        columns.add(col0);
        columns.add(col1);
        columns.add(col2);
        Scheme scheme = new Scheme(columns);

        SeqScanPlan seqScanPlan = new SeqScanPlan(new ArrayList<>(), "table0");
        SeqScanExecutor seqScanExecutor = new SeqScanExecutor(seqScanPlan, diskManager, bufferManager, lockManager, null, txn0);

        Tuple tuple;
        int i = 0;
        while ((tuple = seqScanExecutor.next()) != null) {
            assertEquals(tuple.getValue(scheme, 1), new Integer(i * 3 + 2));
            i++;
        }
    }
}
