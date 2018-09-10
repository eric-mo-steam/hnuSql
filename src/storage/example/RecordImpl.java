package storage.example;

import structure.Record;

public class RecordImpl extends Record {
    /**
     * 偏移记录数
     */
    private long offset;

    public long getOffset() {
        return offset;
    }

    public void setOffset(long offset) {
        this.offset = offset;
    }
}
