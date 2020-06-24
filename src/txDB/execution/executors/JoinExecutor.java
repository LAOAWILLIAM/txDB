package txDB.execution.executors;

import txDB.buffer.BufferManager;
import txDB.concurrency.LockManager;
import txDB.concurrency.Transaction;
import txDB.execution.hash.InMemoryGraceHash;
import txDB.execution.plans.JoinPlan;
import txDB.recovery.LogManager;
import txDB.storage.disk.DiskManager;
import txDB.storage.table.Column;
import txDB.storage.table.RecordID;
import txDB.storage.table.Scheme;
import txDB.storage.table.Tuple;
import txDB.type.Type;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;

/**
 * I use Grace Hash Join here
 */
public class JoinExecutor<K extends Comparable<K>> extends Executor {
    private JoinPlan joinPlan;
    private List<Scheme> schemes;
    private Executor leftChildExecutor;
    private Executor rightChildExecutor;
    private InMemoryGraceHash<K, RecordID> inMemoryGraceHash;

    public JoinExecutor(JoinPlan joinPlan,
                        DiskManager diskManager,
                        BufferManager bufferManager,
                        LockManager lockManager,
                        LogManager logManager,
                        Transaction txn) {
        super(diskManager, bufferManager, lockManager, logManager, txn);
        this.joinPlan = joinPlan;
        this.schemes = joinPlan.getSchemes();
        this.inMemoryGraceHash = new InMemoryGraceHash<>(1000);
        this.initialize();
    }

    public void initialize() {
        if (joinPlan.getChildrenPlanNodes().size() == 0) return;
        leftChildExecutor = this.newExecutor(joinPlan.getChildrenPlanNodes().get(0));
        rightChildExecutor = this.newExecutor(joinPlan.getChildrenPlanNodes().get(1));
        leftNext();
    }

    public Tuple next() {
        return rightNext();
    }

    private void leftNext() {
        Tuple tuple;
        int columnIndex = schemes.get(0).getColumnIndex(joinPlan.getColumnNames().get(0));
        while ((tuple = leftChildExecutor.next()) != null) {
            if (!tuple.isAllocated()) continue;
            inMemoryGraceHash.insert(getValue(schemes.get(0), columnIndex, tuple), tuple.getRecordID());
        }
    }

    private Tuple rightNext() {
        Tuple tuple = rightChildExecutor.next();
        int columnIndex = schemes.get(1).getColumnIndex(joinPlan.getColumnNames().get(1));
//        System.out.println(columnIndex + ", " + tuple.getRecordID().getPageId() + ", " + tuple.getRecordID().getTupleIndex());
        if (tuple == null) return null;
        if (!tuple.isAllocated()) return new Tuple();
        if (inMemoryGraceHash.find(getValue(schemes.get(1), columnIndex, tuple)) != null)
            return tuple;
        return new Tuple();
    }

    @SuppressWarnings("unchecked")
    private <T> T getValue(Scheme scheme, int columnIndex, Tuple tuple) {
        Type.ColumnValueType columnValueType = scheme.getColumn(columnIndex).getColumnValueType();
        byte[] tupleDataPtr = getTupleDataPtr(scheme, columnIndex, tuple.getTupleData(), tuple.getTupleSize());
        return (T) this.deserialize(tupleDataPtr, columnValueType);
    }

    private byte[] getTupleDataPtr(Scheme scheme, int columnIndex, byte[] tupleData, int tupleSize) {
        Column column = scheme.getColumn(columnIndex);
        boolean isInlined = column.isInlined();
        if (isInlined) {
            return Arrays.copyOfRange(tupleData, column.getColumnOffset(), tupleSize);
        }
        return null;
    }

    private Object deserialize(byte[] tupleDataPtr, Type.ColumnValueType columnValueType) {
        ByteBuffer byteBuffer = ByteBuffer.wrap(tupleDataPtr);
        switch (columnValueType) {
            case BOOLEAN:
                return byteBuffer.getChar() == '1';
            case TINYINT:
                // TODO: how to convert char to 1 byte int ???
                return byteBuffer.getChar();
            case SMALLINT:
                return byteBuffer.getShort();
            case INTEGER:
                return byteBuffer.getInt();
            case BIGINT:
                return byteBuffer.getLong();
            case DECIMAL:
                return byteBuffer.getDouble();
            case TIMESTAMP:
                return byteBuffer.get(tupleDataPtr, 0, 8);
            case VARCHAR:
                // TODO: length of VARCHAR should be here.
                return byteBuffer.get(tupleDataPtr, 0, 0);
            default:
                break;
        }
        throw new RuntimeException("Unknown type.");
    }
}
