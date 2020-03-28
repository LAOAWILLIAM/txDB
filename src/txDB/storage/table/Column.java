package txDB.storage.table;

import txDB.type.Type.ColumnValueType;

import java.io.Serializable;

public class Column implements Serializable {
    // TODO
    private String columnName;
    private ColumnValueType columnValueType;
    private int fixedLen;
    private int variableLen;
    private int columnOffset;

    public Column(String columnName, ColumnValueType columnValueType, int fixedLen, int variableLen) {
        this.columnName = columnName;
        this.columnValueType = columnValueType;
        this.fixedLen = fixedLen;
        this.variableLen = variableLen;
    }

    public String getColumnName() {
        return this.columnName;
    }

    public ColumnValueType getColumnValueType() {
        return this.columnValueType;
    }

    public int getFixedLen() {
        return this.fixedLen;
    }

    public int getVariableLen() {
        return this.variableLen;
    }

    public int getColumnOffset() {
        return this.columnOffset;
    }

    public void setColumnOffset(int columnOffset) {
        this.columnOffset = columnOffset;
    }

    public boolean isInlined() {
        return this.columnValueType != ColumnValueType.VARCHAR;
    }
}
