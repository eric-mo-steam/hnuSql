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

    public Record() {

    }

    public int length() {
        return columns.length;
    }

    public Object[] getColumns() {
        return columns;
    }

    public void setColumns(Object[] columns) {
        this.columns = columns;
    }

    public Status getStatus() {
        return status;
    }

    public void setStatus(Status status) {
        this.status = status;
    }
}
