package txDB.storage.page;

import txDB.Config;
import txDB.buffer.BufferManager;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.ListIterator;

public class GraceHashBucketPage<K extends Comparable<K>, V> implements Serializable {
    private int pageId;
    private int nextPageId;
    private int maxSize;
    private List<K> keyList;
    private List<V> valueList;

    // first created
    public GraceHashBucketPage(int pageId, int maxSize) {
        this.pageId = pageId;
        this.nextPageId = Config.INVALID_PAGE_ID;
        this.maxSize = maxSize;
        this.keyList = new ArrayList<>(maxSize);
        this.valueList = new ArrayList<>(maxSize);

    }

    public int getPageId() {
        return pageId;
    }

    public int getNextPageId() {
        return nextPageId;
    }

    public void setNextPageId(int nextPageId) {
        this.nextPageId = nextPageId;
    }

    public boolean insert(K key, V value) {
        if (keyList.size() > maxSize) {
            return false;
        } else {
            keyList.add(key);
            valueList.add(value);
            return true;
        }
    }

    public V find(K key) {
        ListIterator<K> iterator = keyList.listIterator();
        while (iterator.hasNext()) {
            /**
             * unique key supported
             */
            if (iterator.next().compareTo(key) == 0) {
                return valueList.get(iterator.previousIndex());
            }
        }
        return null;
    }
}
