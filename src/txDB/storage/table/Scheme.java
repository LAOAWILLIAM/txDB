package txDB.storage.table;

import java.util.ArrayList;
import java.util.ListIterator;

public class Scheme {
    // TODO
    private int len;
    private ArrayList<Column> columns;

    public int getLen() {
        return this.len;
    }

    public ArrayList<Column> getColumns() {
        return this.columns;
    }

    public int getColumnCount() {
        return this.columns.size();
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
