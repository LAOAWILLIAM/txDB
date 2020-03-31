package txDB.storage.table;

import txDB.type.Type;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;

/**
 * Tuple format:
 * ---------------------------------------------------------------------
 * | FIXED-SIZE or VARIED-SIZED OFFSET | PAYLOAD OF VARIED-SIZED FIELD |
 * ---------------------------------------------------------------------
 */
public class Tuple {
    private byte[] tupleData;
    private RecordID recordID;
    private int tupleSize;
    private boolean isAllocated;

    public Tuple() {}

    public Tuple(ArrayList<Object> values, Scheme scheme) {
        this.tupleSize = scheme.getLen();
        // TODO: variable length should also be considered
        this.tupleData = new byte[this.tupleSize];
        int i;
        for (i = 0; i < scheme.getColumnCount(); i++) {
            // TODO: variable attribute should also be considered
            this.serialize(tupleData, scheme.getColumn(i), values.get(i));
        }
    }

    public Tuple(byte[] tupleData, RecordID recordID, int tupleSize, boolean isAllocated) {
        this.tupleData = tupleData;
        this.recordID = recordID;
        this.tupleSize = tupleSize;
        this.isAllocated = isAllocated;
    }

    public byte[] getTupleData() {
        return this.tupleData;
    }

    public void setTupleData(byte[] tupleData) {
        this.tupleData = tupleData;
    }

    public RecordID getRecordID() {
        return this.recordID;
    }

    public void setRecordID(RecordID recordID) {
        this.recordID = recordID;
    }

    public int getTupleSize() {
        return this.tupleSize;
    }

    public void setTupleSize(int tupleSize) {
        this.tupleSize = tupleSize;
    }

    public boolean isAllocated() {
        return this.isAllocated;
    }

    public void setAllocated(boolean allocated) {
        this.isAllocated = allocated;
    }

    @SuppressWarnings("unchecked")
    public <T> T getValue(Scheme scheme, int columnIndex) {
        Type.ColumnValueType columnValueType = scheme.getColumn(columnIndex).getColumnValueType();
        byte[] tupleDataPtr = getTupleDataPtr(scheme, columnIndex);
        return (T) this.deserialize(tupleDataPtr, columnValueType);
    }

    private byte[] getTupleDataPtr(Scheme scheme, int columnIndex) {
        Column column = scheme.getColumn(columnIndex);
        boolean isInlined = column.isInlined();
        if (isInlined) {
            return Arrays.copyOfRange(this.tupleData, column.getColumnOffset(), this.tupleSize);
        }
        return null;
    }

    /**
     * A simple function to serialize byte data to column value
     * @param tupleData
     * @param column
     * @param value
     */
    private void serialize(byte[] tupleData, Column column, Object value) {
        ByteBuffer byteBuffer = ByteBuffer.wrap(tupleData);
        switch (column.getColumnValueType()) {
            case BOOLEAN:
                byteBuffer.putChar(column.getColumnOffset(), (char) value);
                return;
            case TINYINT:
                byteBuffer.putChar(column.getColumnOffset(), (char) value);
                return;
            case SMALLINT:
                byteBuffer.putShort(column.getColumnOffset(), (short) value);
                return;
            case INTEGER:
                byteBuffer.putInt(column.getColumnOffset(), (int) value);
                return;
            case BIGINT:
                byteBuffer.putLong(column.getColumnOffset(), (long) value);
                return;
            case DECIMAL:
                byteBuffer.putDouble(column.getColumnOffset(), (double) value);
                return;
            case TIMESTAMP:
                byteBuffer.put(tupleData, column.getColumnOffset(), 8);
                return;
            case VARCHAR:
                // TODO
                return;
            default:
                break;
        }
        throw new RuntimeException("Unknown type.");
    }

    /**
     * A simple function to deserialize column value to byte data
     * @param tupleDataPtr
     * @param columnValueType
     * @return
     */
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
