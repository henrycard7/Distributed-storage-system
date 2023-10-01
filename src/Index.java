import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Index {
    private Map<String, IndexEntry> entries;

    public Index() {
        this.entries = new HashMap<>();
    }

    public synchronized boolean add(String fileName) {
        if (entries.containsKey(fileName)) {
            return false;
        } else {
            entries.put(fileName, new IndexEntry(IndexState.UPLOADING));
            return true;
        }
    }

    public synchronized void removeFile(String fileName) {
        IndexEntry entry = entries.get(fileName);
        if (entry != null) {
            entries.remove(fileName);
        }
//        if (entry == null) {
//            return false;
//        } else if (entry.getState() == IndexState.UPLOADING || entry.getState() == IndexState.REMOVING) {
//            return false;
//        } else {
//            entries.remove(fileName);
//            return true;
//        }
    }

    public synchronized IndexState getState(String fileName) {
        IndexEntry entry = entries.get(fileName);
        return entry != null ? entry.getState() : null;
    }

    public synchronized boolean setState(String fileName, IndexState state) {
        IndexEntry entry = entries.get(fileName);
        if (entry == null) {
            return false;
        } else {
            entry.setState(state);
            return true;
        }
    }
    public synchronized List<String> getStoredFiles() {
        ArrayList<String> storedFiles = new ArrayList<String>(entries.keySet());
        for(String s: storedFiles){
            if(!getState(s).equals(IndexState.STORED)){
                storedFiles.remove(s);
            }
        }
        return storedFiles;
//        return new ArrayList<>(entries.keySet());
    }
    public synchronized boolean containsFile(String fileName){
        if(entries.containsKey(fileName)){
            return true;
        }
        else{
            return false;
        }
    }
}

class IndexEntry {
    private IndexState state;

    public IndexEntry(IndexState state) {
        this.state = state;
    }

    public IndexState getState() {
        return state;
    }

    public void setState(IndexState state) {
        this.state = state;
    }
}

enum IndexState {
    UPLOADING,
    STORED,
    REMOVING,
    REMOVED
}
