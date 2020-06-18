package txDB.storage.table;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.ListIterator;

public class Scheme implements Serializable {
    private int len;
    private ArrayList<Column> columns;
    private ArrayList<Column> unlinedColumns;

    public Scheme(ArrayList<Column> columns_) {
        // TODO
        columns = new ArrayList<>();
        unlinedColumns = new ArrayList<>();
        int curOffset = 0, i;
        for (i = 0; i < columns_.size(); i++) {
            Column column = columns_.get(i);
            if (column != null) {
                if (!column.isInlined()) {
                    this.unlinedColumns.add(column);
                } else {
                    column.setColumnOffset(curOffset);
                    curOffset += column.getFixedLen();
                }
                this.columns.add(column);
            }
        }
        this.len = curOffset;
    }

    public int getLen() {
        return this.len;
    }

    public ArrayList<Column> getColumns() {
        return this.columns;
    }

    public ArrayList<Column> getUnlinedColumns() {
        return this.unlinedColumns;
    }

    public int getColumnCount() {
        return this.columns.size();
    }

    public int getUnlinedColumnCount() {
        return this.unlinedColumns.size();
    }

    public int getInlinedColumnCount() {
        return this.columns.size() - this.unlinedColumns.size();
    }

    public Column getColumn(int columnIndex) {
        return this.columns.get(columnIndex);
    }

    public int getColumnIndex(String columnName) {
        ListIterator<Column> iterator = this.columns.listIterator();
        while (iterator.hasNext()) {
            if (iterator.next().getColumnName().equals(columnName)) {
                return iterator.previousIndex();
            }
        }

        return -1;
    }

}
