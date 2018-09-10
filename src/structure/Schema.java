package structure;

import common.Status;

public abstract class Schema {
    /**
     * 表名
     */
    private String tableName;
    /**
     * 表的字段
     */
    private Field[] fields;
    /**
     * 状态
     */
    private Status status;


    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public Field[] getFields() {
        return fields;
    }

    public void setFields(Field[] fields) {
        this.fields = fields;
    }

    public Status getStatus() {
        return status;
    }

    public void setStatus(Status status) {
        this.status = status;
    }
}
