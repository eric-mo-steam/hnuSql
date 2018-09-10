package structure;

/**
 * 工厂接口，负责生产Record、Schema等
 */
public interface Factory {
    /**
     * 生产指定表指定数量的记录
     * @return 记录数组
     */
    Record[] produceRecords(int recordSize, int columnSize);

    /**
     * 生产表模式
     * @return 表模式
     */
    Schema produceSchema();
}
