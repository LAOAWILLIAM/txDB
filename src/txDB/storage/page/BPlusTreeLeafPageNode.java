package txDB.storage.page;

import txDB.Config;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.ListIterator;

public class BPlusTreeLeafPageNode<K extends Comparable<K>, V> extends BPlusTreePageNode<K, V> implements Serializable {
    // TODO
    private int nextPageId;
    private int prevPageId;
    private ArrayList<V> values;

    public BPlusTreeLeafPageNode(K key, V value, int pageId, int parentPageId, int maxSize) {
        setPageId(pageId);
        setParentPageId(parentPageId);
        setIndexPageType(IndexPageType.LEAFPAGE);
        setNextPageId(Config.INVALID_PAGE_ID);
        setPrevPageId(Config.INVALID_PAGE_ID);
        setMaxSize(maxSize);
        keys = new ArrayList<>();
        values = new ArrayList<>();
        keys.add(key);
        values.add(value);
    }

    public BPlusTreeLeafPageNode(List<K> ks, List<V> vs, int pageId, int parentPageId, int maxSize) {
        setPageId(pageId);
        setParentPageId(parentPageId);
        setIndexPageType(IndexPageType.LEAFPAGE);
        setNextPageId(Config.INVALID_PAGE_ID);
        setPrevPageId(Config.INVALID_PAGE_ID);
        setMaxSize(maxSize);
        keys = new ArrayList<>(ks);
        values = new ArrayList<>(vs);
    }

    public int getPrevPageId() {
        return this.prevPageId;
    }

    public void setPrevPageId(int prevPageId) {
        this.prevPageId = prevPageId;
    }

    public int getNextPageId() {
        return this.nextPageId;
    }

    public void setNextPageId(int nextPageId) {
        this.nextPageId = nextPageId;
    }

    public ArrayList<V> getValues() {
        return this.values;
    }

    public void insertAndSort(K key, V value) {
        if (key.compareTo(keys.get(0)) < 0) {
            keys.add(0, key);
            values.add(0, value);
        } else if (key.compareTo(keys.get(keys.size() - 1)) > 0) {
            keys.add(key);
            values.add(value);
        } else {
            ListIterator<K> iterator = keys.listIterator();
            while (iterator.hasNext()) {
                if (iterator.next().compareTo(key) > 0) {
                    int p = iterator.previousIndex();
                    keys.add(p, key);
                    values.add(p, value);
                    break;
                }
            }
        }
    }

    public V getValue(K key) {
        ListIterator<K> iterator = keys.listIterator();
        while (iterator.hasNext()) {
            if (iterator.next().compareTo(key) == 0) {
                return values.get(iterator.previousIndex());
            }
        }
        return null;
    }

    public int getValueIndex(K key) {
        ListIterator<K> iterator = keys.listIterator();
        while (iterator.hasNext()) {
            if (iterator.next().compareTo(key) == 0) {
                return iterator.previousIndex();
            }
        }
        return 0;
    }

    public void remove(K key) {
        int p = keys.indexOf(key);
        keys.remove(p);
        values.remove(p);
    }
}
