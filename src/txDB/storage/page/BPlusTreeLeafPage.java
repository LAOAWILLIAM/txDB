package txDB.storage.page;

import java.io.Serializable;

public class BPlusTreeLeafPage extends BPlusTreePage implements Serializable {
    // TODO
    private int nextPageId;

    public BPlusTreeLeafPage() {
        super();
    }
}
