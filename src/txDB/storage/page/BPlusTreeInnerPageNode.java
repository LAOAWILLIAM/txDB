package txDB.storage.page;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.ListIterator;

public class BPlusTreeInnerPageNode<K extends Comparable<K>, V> extends BPlusTreePageNode<K, V> implements Serializable {
    // TODO
    private ArrayList<Integer> children;

    public BPlusTreeInnerPageNode(K key, int leftPage, int rightPage, int pageId, int parentPageId, int maxSize) {
        setPageId(pageId);
        setParentPageId(parentPageId);
        setIndexPageType(IndexPageType.INNERPAGE);
        setMaxSize(maxSize);
        keys = new ArrayList<>();
        keys.add(key);
        children = new ArrayList<>();
        children.add(leftPage);
        children.add(rightPage);
    }

    public BPlusTreeInnerPageNode(List<K> ks, List<Integer> cd, int pageId, int parentPageId, int maxSize) {
        setPageId(pageId);
        setParentPageId(parentPageId);
        setIndexPageType(IndexPageType.INNERPAGE);
        setMaxSize(maxSize);
        keys = new ArrayList<>(ks);
        children = new ArrayList<>(cd);
    }

    public ArrayList<Integer> getChildren() {
        return this.children;
    }

    public void insertAndSort(K key, int pageId) {
        if (key.compareTo(keys.get(0)) < 0) {
            keys.add(0, key);
            // new right node should be at right side where index is key's index plus 1
            children.add(1, pageId);
        } else if (key.compareTo(keys.get(keys.size() - 1)) > 0) {
            keys.add(key);
            children.add(pageId);
        } else {
            ListIterator<K> iterator = keys.listIterator();
            while (iterator.hasNext()) {
                if (iterator.next().compareTo(key) > 0) {
                    int p = iterator.previousIndex();
                    keys.add(p, key);
                    // new right node should be at right side where index is key's index plus 1
                    children.add(p + 1, pageId);
                    break;
                }
            }
        }
    }

    public void remove(int keyIndex) {
        // TODO: may have bug
        keys.remove(keyIndex);
        children.remove(keyIndex + 1);
    }
}
