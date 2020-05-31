package test.execution;

import txDB.buffer.BufferManager;
import txDB.concurrency.LockManager;
import txDB.concurrency.Transaction;
import txDB.concurrency.TransactionManager;
import txDB.execution.executors.PredEvalExecutor;
import txDB.execution.executors.SeqScanExecutor;
import txDB.execution.plans.Plan;
import txDB.execution.plans.PredEvalPlan;
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
    LockManager lockManager = new LockManager(LockManager.twoPhaseLockType.REGULAR, LockManager.deadlockType.DETECTION);
    TransactionManager transactionManager = new TransactionManager(lockManager, null);

    public ExecutionTest() throws IOException {
//        diskManager.dropFile(dbName);
        diskManager.createFile(dbName);
        diskManager.useFile(dbName);
    }

    @Test
    public void seqScanTest() throws InterruptedException {
        int bufferSize = 10;
        BufferManager bufferManager = new BufferManager(bufferSize, diskManager);
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

        /**
         * select * from table0;
         */
        Tuple tuple;
        int i = 0;
        while ((tuple = seqScanExecutor.next()) != null) {
            assertEquals(tuple.getValue(scheme, 1), new Integer(i * 3 + 2));
            i++;
        }

        columns.clear();
        Column col3 = new Column("col3", Type.ColumnValueType.INTEGER, 4, 0);
        Column col4 = new Column("col4", Type.ColumnValueType.INTEGER, 4, 0);
        columns.add(col0);
        columns.add(col3);
        columns.add(col4);
        scheme = new Scheme(columns);

        seqScanPlan = new SeqScanPlan(new ArrayList<>(), "table1");
        seqScanExecutor = new SeqScanExecutor(seqScanPlan, diskManager, bufferManager, lockManager, null, txn0);

        /**
         * select * from table1;
         */
        i = 0;
        while ((tuple = seqScanExecutor.next()) != null) {
            assertEquals(tuple.getValue(scheme, 1), new Integer(i * 3 + 2));
            i += 15;
        }
    }

    @Test
    public void seqScanWithPredicatesTest() throws InterruptedException {
        int bufferSize = 10;
        BufferManager bufferManager = new BufferManager(bufferSize, diskManager);
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
        ArrayList<Plan> childrenPlanNodes = new ArrayList<>();
        childrenPlanNodes.add(seqScanPlan);
        ArrayList<String> columnNames = new ArrayList<>();
        columnNames.add("col1");
        ArrayList<PredEvalPlan.comparisonType> comparisonTypes = new ArrayList<>();
        comparisonTypes.add(PredEvalPlan.comparisonType.GreaterThanOrEqual);
        ArrayList<Object> values = new ArrayList<>();
        values.add(15002);
        ArrayList<PredEvalPlan.logicType> logicTypes = new ArrayList<>();
        PredEvalPlan predEvalPlan = new PredEvalPlan(childrenPlanNodes, scheme, columnNames, comparisonTypes, logicTypes, values);
        PredEvalExecutor predEvalExecutor = new PredEvalExecutor(predEvalPlan, diskManager, bufferManager, lockManager, null, txn0);

        /**
         * select * from table0 where col1 >= 15002;
         */
        Tuple tuple;
        int i = 5000;
        while ((tuple = predEvalExecutor.next()) != null) {
            if (!tuple.isAllocated()) continue;
            assertEquals(tuple.getValue(scheme, 1), new Integer(i * 3 + 2));
            i++;
        }

        /**
         * select * from table0 where col1 = 15002;
         */
        comparisonTypes.clear();
        comparisonTypes.add(PredEvalPlan.comparisonType.Equal);
        predEvalPlan = new PredEvalPlan(childrenPlanNodes, scheme, columnNames, comparisonTypes, logicTypes, values);
        i = 5000;
        predEvalExecutor.initialize(predEvalPlan);
        while ((tuple = predEvalExecutor.next()) != null) {
            if (!tuple.isAllocated()) continue;
            assertEquals(tuple.getValue(scheme, 1), new Integer(i * 3 + 2));
        }

        /**
         * select * from table0 where col1 != 15002;
         */
        comparisonTypes.clear();
        comparisonTypes.add(PredEvalPlan.comparisonType.NotEqual);
        predEvalPlan = new PredEvalPlan(childrenPlanNodes, scheme, columnNames, comparisonTypes, logicTypes, values);
        i = 5000;
        predEvalExecutor.initialize(predEvalPlan);
        while ((tuple = predEvalExecutor.next()) != null) {
            if (!tuple.isAllocated()) continue;
            assertNotEquals(tuple.getValue(scheme, 1), new Integer(i * 3 + 2));
        }

        /**
         * select * from table0 where col1 > 15002;
         */
        comparisonTypes.clear();
        comparisonTypes.add(PredEvalPlan.comparisonType.GreaterThan);
        predEvalPlan = new PredEvalPlan(childrenPlanNodes, scheme, columnNames, comparisonTypes, logicTypes, values);
        i = 5001;
        predEvalExecutor.initialize(predEvalPlan);
        while ((tuple = predEvalExecutor.next()) != null) {
            if (!tuple.isAllocated()) continue;
            assertEquals(tuple.getValue(scheme, 1), new Integer(i * 3 + 2));
            i++;
        }

        /**
         * select * from table0 where col1 < 15002;
         */
        comparisonTypes.clear();
        comparisonTypes.add(PredEvalPlan.comparisonType.LessThan);
        predEvalPlan = new PredEvalPlan(childrenPlanNodes, scheme, columnNames, comparisonTypes, logicTypes, values);
        i = 0;
        predEvalExecutor.initialize(predEvalPlan);
        while ((tuple = predEvalExecutor.next()) != null) {
            if (!tuple.isAllocated()) continue;
            assertEquals(tuple.getValue(scheme, 1), new Integer(i * 3 + 2));
            i++;
        }
        assertEquals(i, 5000);

        /**
         * select * from table0 where col1 <= 15002;
         */
        comparisonTypes.clear();
        comparisonTypes.add(PredEvalPlan.comparisonType.LessThanOrEqual);
        predEvalPlan = new PredEvalPlan(childrenPlanNodes, scheme, columnNames, comparisonTypes, logicTypes, values);
        i = 0;
        predEvalExecutor.initialize(predEvalPlan);
        while ((tuple = predEvalExecutor.next()) != null) {
            if (!tuple.isAllocated()) continue;
            assertEquals(tuple.getValue(scheme, 1), new Integer(i * 3 + 2));
            i++;
        }
        assertEquals(i, 5001);

        /**
         * select * from table0 where col1 <= 15002 and col2 > 9003;
         */
        comparisonTypes.clear();
        comparisonTypes.add(PredEvalPlan.comparisonType.LessThanOrEqual);
        comparisonTypes.add(PredEvalPlan.comparisonType.GreaterThan);
        columnNames.add("col2");
        values.add(9003);
        logicTypes.add(PredEvalPlan.logicType.AND);
        predEvalPlan = new PredEvalPlan(childrenPlanNodes, scheme, columnNames, comparisonTypes, logicTypes, values);
        i = 3001;
        predEvalExecutor.initialize(predEvalPlan);
        while ((tuple = predEvalExecutor.next()) != null) {
            if (!tuple.isAllocated()) continue;
//            System.out.println(tuple.getValue(scheme, 1) + ", " + tuple.getValue(scheme, 2));
            assertEquals(tuple.getValue(scheme, 1), new Integer(i * 3 + 2));
            i++;
        }
        assertEquals(i, 5001);

        /**
         * select * from table0 where col0 < 1501 or col1 >= 15002;
         */
        comparisonTypes.clear();
        comparisonTypes.add(PredEvalPlan.comparisonType.LessThan);
        comparisonTypes.add(PredEvalPlan.comparisonType.GreaterThanOrEqual);
        columnNames.clear();
        columnNames.add("col0");
        columnNames.add("col1");
        values.clear();
        values.add(1501);
        values.add(15002);
        logicTypes.clear();
        logicTypes.add(PredEvalPlan.logicType.OR);
        predEvalPlan = new PredEvalPlan(childrenPlanNodes, scheme, columnNames, comparisonTypes, logicTypes, values);
        i = 0;
        predEvalExecutor.initialize(predEvalPlan);
        while ((tuple = predEvalExecutor.next()) != null) {
            if (!tuple.isAllocated()) continue;
//            System.out.println(tuple.getValue(scheme, 0) + ", " + tuple.getValue(scheme, 1) + ", " + tuple.getValue(scheme, 2));
            assertEquals(tuple.getValue(scheme, 0), new Integer(i * 3 + 1));
            i++;
            if (i == 500) i = 5000;
        }
        assertEquals(i, 9947);

        diskManager.close();
    }

    @Test
    public void joinTest() {
        int bufferSize = 10;
        BufferManager bufferManager = new BufferManager(bufferSize, diskManager);
        Transaction txn0 = transactionManager.begin();


    }
}
