package structure;

import common.Status;

public abstract class Record {
    /**
     * 记录中各列的值
     */
    private Object[] columns;
    /**
     * 记录的状态
     */
    private Status status;

    public Record(int size) {
        columns = new Object[size];
    }

    public int length() {
        return columns.length;
    }

    public Object getColumn(int index) {
        return columns[index];
    }

    public void setColumn(int index, Object value) {
        columns[index] = value;
    }

    public Status getStatus() {
        return status;
    }

    public void setStatus(Status status) {
        this.status = status;
    }
}
