package storage.example;

import common.Global;
import number.Unsigned;
import storage.Serializer;
import structure.*;

import java.nio.charset.Charset;
import java.util.Arrays;

public class SerializerImpl implements Serializer {

    /**
     * 默认编码的字符集
     */
    private static final Charset charset = Charset.forName("utf32");
    /**
     * 默认缓冲区大小
     */
    private static final int DEFAULT_BUFFER_SIZE = 4096;

    /**
     * 根据字段类型和字面值长度，计算实际存储所需的字节数
     *
     * @param fieldType
     * @param length
     */
    private int getSize(FieldType fieldType, int length) {
        switch (fieldType) {
            case INTEGER:
                return (int) Math.ceil(1.0 * (Math.getExponent(Math.pow(10, length) - 1) + 2) / 8);
            case CHAR:
                return length * 4;
            default:
                return 0;
        }
    }

    @Override
    public byte[] serializeRecord(Schema schema, Record record) {
        return null;
    }

    @Override
    public byte[] serializeSchema(Schema schema) {
        byte[] buf = new byte[DEFAULT_BUFFER_SIZE];
        int pos = 0;
        int recordSize = 0; // 单条记录的存储字节数

        // buf[0, 1]为整个的表模式的存储字节数，最后保存（小端法保存）
        // buf[2, 3]为单个记录的存储字节数，最后保存（小端法保存）
        pos += 4;
        // buf[4, 5, 6, 7]为新记录的偏移记录数，初始化为0
        Arrays.fill(buf, pos, pos + 4, (byte) 0);
        pos += 4;

        // 字段数
        buf[pos++] = (byte) schema.getFields().length;
        for (Field field : schema.getFields()) {
            // 存储字段类型
            buf[pos++] = (byte) field.getFieldType().getValue();
            byte[] chars = field.getName().getBytes(charset);
            // 存储字段名长度
            buf[pos++] = (byte) chars.length;
            // 存储字段名
            System.arraycopy(chars, 0, buf, pos, chars.length);
            // 设置此时的pos位置
            pos += chars.length;
            // 存储字段的字面值长度
            buf[pos++] = (byte) field.getLength();
            // 存储字段的实际存储字节数
            int size = getSize(field.getFieldType(), field.getLength());
            recordSize += size;
            buf[pos++] = (byte) size;
            // 保存该字段的选项
            buf[pos++] = (byte) (((field.isPrimary() ? 1 : 0) << 2) | (field.isKey() ? 1 : 0) << 1);
        }
        // 记录中指示null字段数所需字节：一个有效位加n个字段的null指示位
        recordSize += Math.ceil(1.0 * (1 + schema.getFields().length) / 8);
        // 保存整个表模式的存储字节数
        buf[0] = (byte) (pos & 0xFF);
        buf[1] = (byte) (pos & 0xFF00);
        // 保存单个记录存储字节数
        buf[2] = (byte) (recordSize & 0xFF);
        buf[3] = (byte) (recordSize & 0xFF00);
        return Arrays.copyOf(buf, pos);
    }

    @Override
    public Record deserializeRecord(Schema schema, byte[] bytes) {
        return null;
    }

    @Override
    public Schema deserializeSchema(byte[] bytes) {
        Factory factory = Global.getInstance().getFactory();
        Schema schema = factory.produceSchema();
        SchemaImpl schemaImpl = (SchemaImpl) schema;

        // pos[0, 1]为表格式的存储字节数
        int pos = 2;
        // 读取单条记录的存储字节数
        schemaImpl.setRecordSize(Unsigned.toUnsignedByte(bytes[pos++]) | Unsigned.toUnsignedByte(bytes[pos++]) << 8);
        // 读取新纪录的偏移记录数
        schemaImpl.setNewRecordOffset(
                Unsigned.toUnsignedByte(bytes[pos++]) |
                Unsigned.toUnsignedByte(bytes[pos++]) << 8 |
                Unsigned.toUnsignedByte(bytes[pos++]) << 16 |
                Unsigned.toUnsignedByte(bytes[pos++]) << 20);

        // 读取字段个数
        int fieldSize = Unsigned.toUnsignedByte(bytes[pos++]);
        // 构造字段对象数组
        Field[] fields = new Field[fieldSize];
        for (int i = 0; i < fieldSize; i++) {
            Field field = new Field();
            field.setFieldType(FieldType.valueOf(Unsigned.toUnsignedByte(bytes[pos++])));
            int length = Unsigned.toUnsignedByte(bytes[pos++]);
            field.setName(new String(bytes, pos, length));
            pos += length;
            field.setLength(Unsigned.toUnsignedByte(bytes[pos++]));
            pos++;
            field.setPrimary((bytes[pos] >> 1 & 0x1) == 1);
            field.setKey((bytes[pos] & 0x1) == 1);
            pos++;
            fields[i] = field;
        }
        schema.setFields(fields);
        return schema;
    }
}
