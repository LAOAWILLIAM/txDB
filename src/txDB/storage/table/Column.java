package txDB.storage.table;

public class Column {
    public enum ColumnValueType{INVALID, BOOLEAN, TINYINT, SMALLINT, INTEGER, BIGINT, DECIMAL, VARCHAR, TIMESTAMP}
    // TODO
    private String columnName;
    private ColumnValueType columnValueType;
    private int fixedLen;
    private int variableLen;
    private int columnOffset;

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
}
