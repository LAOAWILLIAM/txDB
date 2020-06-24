package test.execution;

import txDB.buffer.BufferManager;
import txDB.concurrency.LockManager;
import txDB.concurrency.Transaction;
import txDB.concurrency.TransactionManager;
import txDB.execution.executors.JoinExecutor;
import txDB.execution.executors.PredEvalExecutor;
import txDB.execution.executors.SeqScanExecutor;
import txDB.execution.plans.JoinPlan;
import txDB.execution.plans.Plan;
import txDB.execution.plans.PredEvalPlan;
import txDB.execution.plans.SeqScanPlan;
import txDB.recovery.LogManager;
import txDB.storage.disk.DiskManager;
import txDB.storage.table.Column;
import txDB.storage.table.Scheme;
import txDB.storage.table.Tuple;
import txDB.type.Type;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;

import org.junit.Test;
import static org.junit.Assert.*;

public class ExecutionTest {
    // TODO
    String dbName = "test";
    DiskManager diskManager = new DiskManager();
    LockManager lockManager = new LockManager(LockManager.TwoPhaseLockType.REGULAR, LockManager.DeadlockType.DETECTION);
    LogManager logManager = new LogManager(diskManager);
    TransactionManager transactionManager = new TransactionManager(lockManager, null);

    public ExecutionTest() throws IOException {
//        diskManager.dropFile(dbName);
        diskManager.createFile(dbName);
        diskManager.useFile(dbName);
    }

    @Test
    public void seqScanTest() {
        int bufferSize = 1000;
        BufferManager bufferManager = new BufferManager(bufferSize, diskManager, logManager);
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
        Instant start = Instant.now();
        while ((tuple = seqScanExecutor.next()) != null) {
            assertEquals(tuple.getValue(scheme, 1), new Integer(i * 3 + 2));
            i++;
        }
        Instant end = Instant.now();
        Duration timeElapsed = Duration.between(start, end);
        System.out.println("Time elapsed: " + timeElapsed.toMillis());
        assertEquals(10000, i);

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
        start = Instant.now();
        while ((tuple = seqScanExecutor.next()) != null) {
            assertEquals(tuple.getValue(scheme, 1), new Integer(i * 3 + 2));
            i += 15;
        }
        end = Instant.now();
        timeElapsed = Duration.between(start, end);
        System.out.println("Time elapsed: " + timeElapsed.toMillis());
        assertEquals(7500, i);

        seqScanPlan = new SeqScanPlan(new ArrayList<>(), "table2");
        seqScanExecutor = new SeqScanExecutor(seqScanPlan, diskManager, bufferManager, lockManager, null, txn0);

        /**
         * select * from table2;
         */
        i = 0;
        start = Instant.now();
        while ((tuple = seqScanExecutor.next()) != null) {
            assertEquals(tuple.getValue(scheme, 1), new Integer(i * 3 + 2));
            i += 15;
        }
        end = Instant.now();
        timeElapsed = Duration.between(start, end);
        System.out.println("Time elapsed: " + timeElapsed.toMillis());
        assertEquals(1500, i);

        diskManager.close();
        lockManager.closeDetection();
    }

    @Test
    public void seqScanWithPredicatesTest() {
        int bufferSize = 10;
        BufferManager bufferManager = new BufferManager(bufferSize, diskManager, logManager);
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
        assertEquals(i, 10000);

        diskManager.close();
        lockManager.closeDetection();
    }

    @Test
    public void twoTablejoinTest() throws InterruptedException {
        int bufferSize = 100;
        BufferManager bufferManager = new BufferManager(bufferSize, diskManager, logManager);
        Transaction txn0 = transactionManager.begin();

        ArrayList<Column> columns = new ArrayList<>();
        Column col0 = new Column("col0", Type.ColumnValueType.INTEGER, 4, 0);
        Column col1 = new Column("col1", Type.ColumnValueType.INTEGER, 4, 0);
        Column col2 = new Column("col2", Type.ColumnValueType.INTEGER, 4, 0);
        columns.add(col0);
        columns.add(col1);
        columns.add(col2);
        Scheme scheme0 = new Scheme(columns);

        col0 = new Column("col0", Type.ColumnValueType.INTEGER, 4, 0);
        Column col3 = new Column("col3", Type.ColumnValueType.INTEGER, 4, 0);
        Column col4 = new Column("col4", Type.ColumnValueType.INTEGER, 4, 0);
        columns.clear();
        columns.add(col0);
        columns.add(col3);
        columns.add(col4);
        Scheme scheme1 = new Scheme(columns);

        ArrayList<Scheme> schemes = new ArrayList<>();
        schemes.add(scheme1);
        schemes.add(scheme0);

        SeqScanPlan seqScanPlan0 = new SeqScanPlan(new ArrayList<>(), "table0");
        SeqScanPlan seqScanPlan1 = new SeqScanPlan(new ArrayList<>(), "table1");
        ArrayList<Plan> childrenPlanNodes = new ArrayList<>();
        childrenPlanNodes.add(seqScanPlan1);
        childrenPlanNodes.add(seqScanPlan0);
        ArrayList<String> columnNames = new ArrayList<>();
        columnNames.add("col0");
        columnNames.add("col0");

        JoinPlan joinPlan = new JoinPlan(childrenPlanNodes, schemes, columnNames);
        JoinExecutor<Integer> joinExecutor = new JoinExecutor<>(joinPlan, diskManager, bufferManager, lockManager, null, txn0);

        Tuple tuple;
        int i = 0;
        Instant start = Instant.now();
        while ((tuple = joinExecutor.next()) != null) {
            if (!tuple.isAllocated()) continue;
            assertEquals(tuple.getValue(scheme1, 0), new Integer(i * 3 + 1));
            i += 15;
        }
        Instant end = Instant.now();
        Duration timeElapsed = Duration.between(start, end);
        System.out.println("Time elapsed: " + timeElapsed.toMillis());
        assertEquals(i, 7500);

        diskManager.close();
        lockManager.closeDetection();
    }

    @Test
    public void threeTableJoinTest() throws InterruptedException {
        int bufferSize = 1000;
        BufferManager bufferManager = new BufferManager(bufferSize, diskManager, logManager);
        Transaction txn0 = transactionManager.begin();

        ArrayList<Column> columns = new ArrayList<>();
        Column col0 = new Column("col0", Type.ColumnValueType.INTEGER, 4, 0);
        Column col1 = new Column("col1", Type.ColumnValueType.INTEGER, 4, 0);
        Column col2 = new Column("col2", Type.ColumnValueType.INTEGER, 4, 0);
        columns.add(col0);
        columns.add(col1);
        columns.add(col2);
        Scheme scheme0 = new Scheme(columns);

        col0 = new Column("col0", Type.ColumnValueType.INTEGER, 4, 0);
        Column col3 = new Column("col3", Type.ColumnValueType.INTEGER, 4, 0);
        Column col4 = new Column("col4", Type.ColumnValueType.INTEGER, 4, 0);
        columns.clear();
        columns.add(col0);
        columns.add(col3);
        columns.add(col4);
        Scheme scheme1 = new Scheme(columns);

        ArrayList<Scheme> schemes = new ArrayList<>();
        schemes.add(scheme1);
        schemes.add(scheme0);

        SeqScanPlan seqScanPlan0 = new SeqScanPlan(new ArrayList<>(), "table0");
        SeqScanPlan seqScanPlan1 = new SeqScanPlan(new ArrayList<>(), "table1");
        SeqScanPlan seqScanPlan2 = new SeqScanPlan(new ArrayList<>(), "table2");

        ArrayList<String> columnNames = new ArrayList<>();
        columnNames.add("col0");
        columnNames.add("col0");

        ArrayList<Plan> childrenPlanNodes1 = new ArrayList<>();
        childrenPlanNodes1.add(seqScanPlan1);
        childrenPlanNodes1.add(seqScanPlan0);

        JoinPlan joinPlan1 = new JoinPlan(childrenPlanNodes1, schemes, columnNames);

        ArrayList<Plan> childrenPlanNodes0 = new ArrayList<>();
        childrenPlanNodes0.add(seqScanPlan2);
        childrenPlanNodes0.add(joinPlan1);

        JoinPlan joinPlan0 = new JoinPlan(childrenPlanNodes0, schemes, columnNames);

        JoinExecutor<Integer> joinExecutor = new JoinExecutor<>(joinPlan0, diskManager, bufferManager, lockManager, null, txn0);

        Tuple tuple;
        int i = 0;
        Instant start = Instant.now();
        while ((tuple = joinExecutor.next()) != null) {
            if (!tuple.isAllocated()) continue;
            assertEquals(tuple.getValue(scheme1, 0), new Integer(i * 3 + 1));
            i += 15;
        }
        Instant end = Instant.now();
        Duration timeElapsed = Duration.between(start, end);
        System.out.println("Time elapsed: " + timeElapsed.toMillis());
        assertEquals(i, 1500);

        diskManager.close();
        lockManager.closeDetection();
    }
}
