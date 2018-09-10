package storage;

import structure.Record;
import structure.Schema;

public interface Serializer {
    /**
     * 按照表模式的格式，将一条记录序列化为字节数组
     * @param schema 表模式
     * @param record 记录
     * @return 字节数组
     */
    byte[] serializeRecord(Schema schema, Record record);

    /**
     * 将一个表模式序列化为字节数组
     * @param schema 指定的表模式
     * @return 字节数组
     */
    byte[] serializeSchema(Schema schema);

    /**
     * 按照表模式的格式，将字节数组反序列化为一条记录
     * @param schema 表模式
     * @param bytes 字节数组
     * @return 记录对象
     */
    Record deserializeRecord(Schema schema, byte[] bytes);

    /**
     * 将字节数组反序列化为表模式
     * @param bytes
     * @return 表模式
     */
    Schema deserializeSchema(byte[] bytes);
}
