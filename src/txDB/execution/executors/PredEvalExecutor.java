package txDB.execution.executors;

import txDB.buffer.BufferManager;
import txDB.concurrency.LockManager;
import txDB.concurrency.Transaction;
import txDB.execution.plans.Plan;
import txDB.execution.plans.PredEvalPlan;
import txDB.recovery.LogManager;
import txDB.storage.disk.DiskManager;
import txDB.storage.table.Column;
import txDB.storage.table.Scheme;
import txDB.storage.table.Tuple;
import txDB.type.Type;

import java.nio.ByteBuffer;
import java.util.Arrays;

public class PredEvalExecutor extends Executor {
    private PredEvalPlan predEvalPlan;
    private Executor childExecutor;
    private Scheme scheme;
    private boolean existOr;

    public PredEvalExecutor(PredEvalPlan predEvalPlan,
                            DiskManager diskManager,
                            BufferManager bufferManager,
                            LockManager lockManager,
                            LogManager logManager,
                            Transaction txn) throws InterruptedException {
        super(diskManager, bufferManager, lockManager, logManager, txn);
        this.predEvalPlan = predEvalPlan;
        this.scheme = predEvalPlan.getScheme();
        this.existOr = this.predEvalPlan.getLogicTypes().contains(PredEvalPlan.logicType.OR);
        this.initialize();
    }

    public void initialize() throws InterruptedException {
        if (predEvalPlan.getChildrenPlanNodes().size() == 0) return;
        Plan plan = predEvalPlan.getChildrenPlanNodes().get(0);
        childExecutor = this.newExecutor(plan);
    }

    public void initialize(PredEvalPlan predEvalPlan) throws InterruptedException {
        if (predEvalPlan.getChildrenPlanNodes().size() == 0) return;
        this.predEvalPlan = predEvalPlan;
        this.existOr = this.predEvalPlan.getLogicTypes().contains(PredEvalPlan.logicType.OR);
        Plan plan = predEvalPlan.getChildrenPlanNodes().get(0);
        childExecutor = this.newExecutor(plan);
    }

    public Tuple next() throws InterruptedException {
        Tuple tuple = childExecutor.next();
        if (tuple == null) return null;
        int i = 0;
        // TODO: can handle all AND and all OR, but not combination of AND and OR; expression tree needed
        if (existOr) {
            for (String columnName: predEvalPlan.getColumnNames()) {
                int columnIndex = scheme.getColumnIndex(columnName);
                Type.ColumnValueType columnValueType = scheme.getColumn(columnIndex).getColumnValueType();
                byte[] tupleDataPtr = getTupleDataPtr(scheme, columnIndex, tuple.getTupleData(), tuple.getTupleSize());
                if (predEvalPlan.getComparisonTypes().get(i) == PredEvalPlan.comparisonType.GreaterThan) {
                    if (compareGreaterThan(tupleDataPtr, predEvalPlan.getValues().get(i), columnValueType))
                        return tuple;
                } else if (predEvalPlan.getComparisonTypes().get(i) == PredEvalPlan.comparisonType.Equal) {
                    if (compareEqual(tupleDataPtr, predEvalPlan.getValues().get(i), columnValueType))
                        return tuple;
                } else if (predEvalPlan.getComparisonTypes().get(i) == PredEvalPlan.comparisonType.NotEqual) {
                    if (compareNotEqual(tupleDataPtr, predEvalPlan.getValues().get(i), columnValueType))
                        return tuple;
                } else if (predEvalPlan.getComparisonTypes().get(i) == PredEvalPlan.comparisonType.GreaterThanOrEqual) {
                    if (compareGreaterThanOrEqual(tupleDataPtr, predEvalPlan.getValues().get(i), columnValueType))
                        return tuple;
                } else if (predEvalPlan.getComparisonTypes().get(i) == PredEvalPlan.comparisonType.LessThan) {
                    if (compareLessThan(tupleDataPtr, predEvalPlan.getValues().get(i), columnValueType))
                        return tuple;
                } else if (predEvalPlan.getComparisonTypes().get(i) == PredEvalPlan.comparisonType.LessThanOrEqual) {
                    if (compareLessThanOrEqual(tupleDataPtr, predEvalPlan.getValues().get(i), columnValueType))
                        return tuple;
                }
//                System.out.println(tuple.getRecordID().getPageId() + ", " + tuple.getRecordID().getTupleIndex());
                i++;
            }
            return new Tuple();
        } else {
            for (String columnName: predEvalPlan.getColumnNames()) {
                int columnIndex = scheme.getColumnIndex(columnName);
                Type.ColumnValueType columnValueType = scheme.getColumn(columnIndex).getColumnValueType();
                byte[] tupleDataPtr = getTupleDataPtr(scheme, columnIndex, tuple.getTupleData(), tuple.getTupleSize());
                if (predEvalPlan.getComparisonTypes().get(i) == PredEvalPlan.comparisonType.GreaterThan) {
                    if (!compareGreaterThan(tupleDataPtr, predEvalPlan.getValues().get(i), columnValueType))
                        return new Tuple();
                } else if (predEvalPlan.getComparisonTypes().get(i) == PredEvalPlan.comparisonType.Equal) {
                    if (!compareEqual(tupleDataPtr, predEvalPlan.getValues().get(i), columnValueType))
                        return new Tuple();
                } else if (predEvalPlan.getComparisonTypes().get(i) == PredEvalPlan.comparisonType.NotEqual) {
                    if (!compareNotEqual(tupleDataPtr, predEvalPlan.getValues().get(i), columnValueType))
                        return new Tuple();
                } else if (predEvalPlan.getComparisonTypes().get(i) == PredEvalPlan.comparisonType.GreaterThanOrEqual) {
                    if (!compareGreaterThanOrEqual(tupleDataPtr, predEvalPlan.getValues().get(i), columnValueType))
                        return new Tuple();
                } else if (predEvalPlan.getComparisonTypes().get(i) == PredEvalPlan.comparisonType.LessThan) {
                    if (!compareLessThan(tupleDataPtr, predEvalPlan.getValues().get(i), columnValueType))
                        return new Tuple();
                } else if (predEvalPlan.getComparisonTypes().get(i) == PredEvalPlan.comparisonType.LessThanOrEqual) {
                    if (!compareLessThanOrEqual(tupleDataPtr, predEvalPlan.getValues().get(i), columnValueType))
                        return new Tuple();
                }
                i++;
            }
            return tuple;
        }
    }

    private byte[] getTupleDataPtr(Scheme scheme, int columnIndex, byte[] tupleData, int tupleSize) {
        Column column = scheme.getColumn(columnIndex);
        boolean isInlined = column.isInlined();
        if (isInlined) {
            return Arrays.copyOfRange(tupleData, column.getColumnOffset(), tupleSize);
        }
        return null;
    }

    private boolean compareGreaterThan(byte[] tupleDataPtr, Object value, Type.ColumnValueType columnValueType) {
        ByteBuffer byteBuffer = ByteBuffer.wrap(tupleDataPtr);
        switch (columnValueType) {
            case BOOLEAN:
                return byteBuffer.getChar() > (char) value;
            case TINYINT:
                // TODO: how to convert char to 1 byte int ???
                return byteBuffer.getChar() > (char) value;
            case SMALLINT:
                return byteBuffer.getShort() > (short) value;
            case INTEGER:
                return byteBuffer.getInt() > (int) value;
            case BIGINT:
                return byteBuffer.getLong() > (long) value;
            case DECIMAL:
                return byteBuffer.getDouble() > (double) value;
//            case TIMESTAMP:
//                return byteBuffer.get(tupleDataPtr, 0, 8) > ;
//            case VARCHAR:
//                // TODO: length of VARCHAR should be here.
//                return byteBuffer.get(tupleDataPtr, 0, 0) > ;
            default:
                break;
        }
        throw new RuntimeException("Unknown type.");
    }

    private boolean compareEqual(byte[] tupleDataPtr, Object value, Type.ColumnValueType columnValueType) {
        ByteBuffer byteBuffer = ByteBuffer.wrap(tupleDataPtr);
        switch (columnValueType) {
            case BOOLEAN:
                return byteBuffer.getChar() == (char) value;
            case TINYINT:
                // TODO: how to convert char to 1 byte int ???
                return byteBuffer.getChar() == (char) value;
            case SMALLINT:
                return byteBuffer.getShort() == (short) value;
            case INTEGER:
                return byteBuffer.getInt() == (int) value;
            case BIGINT:
                return byteBuffer.getLong() == (long) value;
            case DECIMAL:
                return byteBuffer.getDouble() == (double) value;
//            case TIMESTAMP:
//                return byteBuffer.get(tupleDataPtr, 0, 8) == ;
//            case VARCHAR:
//                // TODO: length of VARCHAR should be here.
//                return byteBuffer.get(tupleDataPtr, 0, 0) == ;
            default:
                break;
        }
        throw new RuntimeException("Unknown type.");
    }

    private boolean compareNotEqual(byte[] tupleDataPtr, Object value, Type.ColumnValueType columnValueType) {
        return !compareEqual(tupleDataPtr, value, columnValueType);
    }

    private boolean compareGreaterThanOrEqual(byte[] tupleDataPtr, Object value, Type.ColumnValueType columnValueType) {
        return compareGreaterThan(tupleDataPtr, value, columnValueType) || compareEqual(tupleDataPtr, value, columnValueType);
    }

    private boolean compareLessThan(byte[] tupleDataPtr, Object value, Type.ColumnValueType columnValueType) {
        return !compareGreaterThanOrEqual(tupleDataPtr, value, columnValueType);
    }

    private boolean compareLessThanOrEqual(byte[] tupleDataPtr, Object value, Type.ColumnValueType columnValueType) {
        return compareLessThan(tupleDataPtr, value, columnValueType) || compareEqual(tupleDataPtr, value, columnValueType);
    }
}
