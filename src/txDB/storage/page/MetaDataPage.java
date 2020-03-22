package txDB.storage.page;

import txDB.storage.table.Scheme;

import java.io.Serializable;
import java.util.HashMap;

public class MetaDataPage implements Serializable {
    // TODO
    private int nextPageId;
    private int prevPageId;
    private HashMap<String, RelationMetaData> relationMetaDataMap;
    private HashMap<String, IndexMetaData> indexMetaDataMap;

    public MetaDataPage() {

    }

    public void addRelationMetaData(String relationName, RelationMetaData relationMetaData) {
        this.relationMetaDataMap.put(relationName, relationMetaData);
    }

    public void addIndexMetaData(String indexName, IndexMetaData indexMetaData) {
        this.indexMetaDataMap.put(indexName, indexMetaData);
    }

    private class RelationMetaData {
        public Scheme scheme;
        public String relationName;
        public int rootRelationPageId;
    }

    private class IndexMetaData {
        public String indexName;
        public int rootIndexPageId;
    }
}
