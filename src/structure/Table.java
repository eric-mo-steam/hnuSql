package structure;

import common.Global;

/**
 * 数据表
 */
public class Table {
    /**
     * 记录组所属表的表格式
     */
    private Schema schema;
    /**
     * 记录数组
     */
    private Record[] records;

    public Table(Schema schema, int size) {
        Factory factory = Global.getInstance().getFactory();
        this.records = factory.produceRecords(size);
        this.schema = schema;
    }

    public Schema getSchema() {
        return schema;
    }

    public Record[] getRecords() {
        return records;
    }
}
