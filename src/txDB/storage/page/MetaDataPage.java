package txDB.storage.page;

import com.sun.tools.javac.util.List;
import txDB.storage.table.Scheme;
import txDB.storage.table.Column;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;

public class MetaDataPage implements Serializable {
    // TODO
    private int nextPageId;
    private int prevPageId;
    private HashMap<String, RelationMetaData> relationMetaDataMap;
    private HashMap<String, IndexMetaData> indexMetaDataMap;
    // maybe useful in future
    private ArrayList<Integer> freeList;

    public MetaDataPage() {
        this.relationMetaDataMap = new HashMap<>();
        this.indexMetaDataMap = new HashMap<>();
        this.freeList = new ArrayList<>();
    }

    public MetaDataPage(ArrayList<Integer> freeList) {
        this.relationMetaDataMap = new HashMap<>();
        this.indexMetaDataMap = new HashMap<>();
        this.freeList = freeList;
    }

    public void addRelationMetaData(String relationName, RelationMetaData relationMetaData) {
        this.relationMetaDataMap.put(relationName, relationMetaData);
    }

    public void addIndexMetaData(String indexName, IndexMetaData indexMetaData) {
        this.indexMetaDataMap.put(indexName, indexMetaData);
    }

    public RelationMetaData getRelationMetaData(String relationName) {
        return this.relationMetaDataMap.get(relationName);
    }

    public IndexMetaData getIndexMetaData(String indexName) {
        return this.indexMetaDataMap.get(indexName);
    }

    // maybe useful in future
    public ArrayList<Integer> getFreeList() {
        return this.freeList;
    }

    // maybe useful in future
    public void updateFreeList(ArrayList<Integer> freeList) {
        this.freeList = freeList;
    }

    public class RelationMetaData implements Serializable {
        public Scheme scheme;
        public String relationName;
        public int rootRelationPageId;

        public RelationMetaData(Scheme scheme, String relationName, int rootRelationPageId) {
            this.scheme = scheme;
            this.relationName = relationName;
            this.rootRelationPageId = rootRelationPageId;
        }
    }

    public class IndexMetaData implements Serializable {
        public String indexName;
        public String relationName;
        public ArrayList<Column> indexAttributes;
        public int rootIndexPageId;

        public IndexMetaData(String indexName, String relationName, ArrayList<Column> indexAttributes, int rootIndexPageId) {
            this.indexName = indexName;
            this.relationName = relationName;
            this.indexAttributes = new ArrayList<>(indexAttributes);
            this.rootIndexPageId = rootIndexPageId;
        }
    }
}
