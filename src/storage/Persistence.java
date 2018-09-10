package storage;

import structure.Field;
import structure.Schema;
import structure.Table;

public interface Persistence {

    /**
     * 写入一个（新的或是修改过的）表模式
     * @param schema 表模式对象
     * @return 是否写入成功
     */
    boolean writeSchema(Schema schema);

    /**
     * 从硬盘中加载所有的表模式（默认内存可以存下所有表模式）
     * @return 所有表模式所构成的数组
     */
    Schema[] loadSchemas();

    /**
     * 打开某张表
     * @param table 指定的表
     */
    void open(Table table);

    /**
     * 关闭某张表
     * @param table 指定的表
     */
    void close(Table table);

    /**
     * 向数据表中，加载记录
     * @return 实际加载的记录个数
     */
    long loadRecords(Table table);

    /**
     * 将数据表中的更新或是新增，写入表中
     * @param table 指定的数据表
     * @return 更新或是新增的记录个数
     */
    long writeRecords(Table table);

    /**
     * 通过索引，向数据表中加载记录
     * @param table 记录的缓冲区
     * @param indexs 索引组合
     * @param lowerBounds 索引组合值的下界
     * @param upperBounds 索引组合值的上界
     * @return 实际加载的记录个数
     */
    long loadRecordsById(Table table, Field[] indexs, Object[] lowerBounds, Object[] upperBounds);
}
