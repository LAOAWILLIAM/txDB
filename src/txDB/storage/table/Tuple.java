package txDB.storage.table;

import txDB.type.Type;

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;

/**
 * Tuple format:
 * ---------------------------------------------------------------------
 * | FIXED-SIZE or VARIED-SIZED OFFSET | PAYLOAD OF VARIED-SIZED FIELD |
 * ---------------------------------------------------------------------
 */
public class Tuple implements Serializable {
    private byte[] tupleData;
    private RecordID recordID;
    private int tupleSize;
    private boolean isAllocated;

    public Tuple() {}

    // tuple without unlined columns
    public Tuple(ArrayList<Object> values, Scheme scheme) {
        this.tupleSize = scheme.getLen();
        this.tupleData = new byte[this.tupleSize];
        int i;
        for (i = 0; i < scheme.getColumnCount(); i++) {
            this.serialize(tupleData, scheme.getColumn(i), values.get(i), -1, -1);
        }
    }

    // tuple with unlined columns
    public Tuple(ArrayList<Object> values, Scheme scheme, ArrayList<Integer> unlinedValueLens) {
        this.tupleSize = scheme.getLen();
        int i, curOffset = scheme.getLen();
        for (i = 0; i < unlinedValueLens.size(); i++) {
            tupleSize += unlinedValueLens.get(i) + 4;
        }
        this.tupleData = new byte[this.tupleSize];
        for (i = 0; i < scheme.getColumnCount(); i++) {
            Column column = scheme.getColumn(i);
            if (!column.isInlined()) {
                this.serialize(tupleData, column, values.get(i), unlinedValueLens.get(i - scheme.getInlinedColumnCount()), curOffset);
                curOffset += unlinedValueLens.get(i - scheme.getInlinedColumnCount()) + 4;
            } else {
                this.serialize(tupleData, column, values.get(i), -1, -1);
            }
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
//        System.out.println(Arrays.toString(tupleDataPtr));
        return (T) this.deserialize(tupleDataPtr, columnValueType, columnIndex, scheme.getInlinedColumnCount(), scheme.getColumnCount());
    }

    private byte[] getTupleDataPtr(Scheme scheme, int columnIndex) {
        Column column = scheme.getColumn(columnIndex);
        boolean isInlined = column.isInlined();
        if (isInlined) {
            return Arrays.copyOfRange(this.tupleData, column.getColumnOffset(), this.tupleSize);
        } else {
            return Arrays.copyOfRange(this.tupleData, scheme.getLen(), this.tupleSize);
        }
    }

    /**
     * A simple function to serialize byte data to column value
     * @param tupleData
     * @param column
     * @param value
     */
    private void serialize(byte[] tupleData, Column column, Object value, int unlinedValueLen, int curOffset) {
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
                // TODO
                return;
            case VARCHAR:
                // TODO
//                StringBuilder stringBuilder = new StringBuilder(value.toString());
                byteBuffer.putInt(curOffset, unlinedValueLen);
                curOffset += 4;
                byte[] val = value.toString().getBytes();
                for (int i = 0; i < val.length; i++) {
                    byteBuffer.put(curOffset + i, val[i]);
                }
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
    private Object deserialize(byte[] tupleDataPtr, Type.ColumnValueType columnValueType, int columnIndex, int inLinedColumnSize, int columnSize) {
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
                // TODO
                return byteBuffer.get(tupleDataPtr, 0, 8);
            case VARCHAR:
                // TODO: length of VARCHAR should be here.
                int i = inLinedColumnSize, offset = 0;
                for (; i < columnSize; i++) {
                    int varCharLen = byteBuffer.getInt(offset);
                    offset += 4;
                    if (columnIndex == i) {
                        byte[] varChar = new byte[varCharLen];
                        for (int j = 0; j < varCharLen; j++) {
                            varChar[j] = byteBuffer.get(offset + j);
                        }
//                        String str = new String(varChar);
//                        System.out.println(str);
                        return new String(varChar);
                    }
                    offset += varCharLen;
                }
                return null;
            default:
                break;
        }
        throw new RuntimeException("Unknown type.");
    }
}
