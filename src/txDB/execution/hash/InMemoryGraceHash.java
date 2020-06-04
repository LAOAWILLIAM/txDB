package txDB.execution.hash;

import java.util.ArrayList;
import java.util.List;
import java.util.ListIterator;

/**
 * It is a in-memory grace hash table
 * @param <K>
 * @param <V>
 */
public class InMemoryGraceHash<K extends Comparable<K>, V> {
    private int bucketSize;
    private GraceHashHeader<K, V> graceHashHeader;

    // first hash
    public InMemoryGraceHash(int bucketSize) {
        this.bucketSize = bucketSize;
        graceHashHeader = new GraceHashHeader<>(bucketSize);
    }

    private class GraceHashHeader<K extends Comparable<K>, V> {
        private int bucketSize;
        private GraceHashBucket<K, V>[] graceHashBucketList;

        public GraceHashHeader(int bucketSize) {
            this.bucketSize = bucketSize;
            this.graceHashBucketList = new GraceHashBucket[bucketSize];
        }
    }

    private class GraceHashBucket<K extends Comparable<K>, V> {
        private GraceHashBucket<K, V> prevBucket;
        private GraceHashBucket<K, V> nextBucket;
        private List<K> keyList;
        private List<V> valueList;
        private int maxSize;

        public GraceHashBucket(int maxSize) {
            this.maxSize = maxSize;
            this.keyList = new ArrayList<>(maxSize);
            this.valueList = new ArrayList<>(maxSize);
        }

        public boolean insert(K key, V value) {
//            System.out.println("insert " + key.toString());
            if (keyList.size() + 1 > maxSize) {
                return false;
            } else {
                keyList.add(key);
                valueList.add(value);
                return true;
            }
        }

        public V find(K key) {
//            System.out.println("find " + key.toString());
            ListIterator<K> iterator = keyList.listIterator();
            while (iterator.hasNext()) {
                /**
                 * unique key supported
                 */
                if (iterator.next().compareTo(key) == 0) {
//                    System.out.println("yes");
                    return valueList.get(iterator.previousIndex());
                }
            }
            return null;
        }
    }

    public void insert(K key, V value) {
        int index = hash(key);
        GraceHashBucket<K, V> graceHashBucket;
        if (graceHashHeader.graceHashBucketList[index] == null) {
            graceHashBucket = new GraceHashBucket<>(200);
            graceHashHeader.graceHashBucketList[index] = graceHashBucket;
        } else {
            graceHashBucket = graceHashHeader.graceHashBucketList[index];
        }

        while (!graceHashBucket.insert(key, value)) {
//            System.out.println("create next node");
            GraceHashBucket<K, V> nextGraceHashBucket = graceHashBucket.nextBucket;
            if (nextGraceHashBucket == null) {
                nextGraceHashBucket = new GraceHashBucket<>(200);
                graceHashBucket.nextBucket = nextGraceHashBucket;
                nextGraceHashBucket.prevBucket = graceHashBucket;
            }
            graceHashBucket = nextGraceHashBucket;
        }
    }

    public V find(K key) {
        int index = hash(key);
        V res;
        GraceHashBucket<K, V> graceHashBucket = graceHashHeader.graceHashBucketList[index];
        if (graceHashBucket == null) {
            return null;
        } else {
            res = graceHashBucket.find(key);
            while (res == null) {
                graceHashBucket = graceHashBucket.nextBucket;
                if (graceHashBucket == null) break;
                res = graceHashBucket.find(key);
            }
        }
        return res;
    }

    public void delete(K key) {
        // TODO
    }

    private int hash(K key) {
        return (Integer) key % bucketSize;
    }
}
