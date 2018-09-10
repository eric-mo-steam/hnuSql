package storage.example;

import structure.Schema;

public class SchemaImpl extends Schema {
    /**
     * 每个记录的存储字节数
     */
    private int recordSize;
    /**
     * 新记录的偏移记录数
     */
    private long newRecordOffset;

    public long getNewRecordOffset() {
        return newRecordOffset;
    }

    public void setNewRecordOffset(long newRecordOffset) {
        this.newRecordOffset = newRecordOffset;
    }

    public int getRecordSize() {
        return recordSize;
    }

    public void setRecordSize(int recordSize) {
        this.recordSize = recordSize;
    }
}
