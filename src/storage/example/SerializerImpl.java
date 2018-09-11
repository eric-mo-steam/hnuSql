package storage.example;

import common.Global;
import common.Status;
import exception.BizException;
import exception.StorageException;
import number.Unsigned;
import storage.Serializer;
import structure.*;

import java.math.BigInteger;
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
     * 计算指定字段的存储字节数
     * @param field 指定的字段
     * @return 所需的存储字节数
     */
    private int getStoreSize(Field field) {
        FieldType fieldType = field.getFieldType();
        int length = field.getLength();
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
        SchemaImpl schemaImpl = (SchemaImpl) schema;
        if (schema == null) {
            throw new BizException("No Schema in record");
        }

        byte[] buffer = new byte[schemaImpl.getRecordSize()];
        if (record.getStatus() == Status.DELETE) {
            return buffer;
        }

        Field[] fields = schema.getFields();
        // 略过前导的标志位字节
        int pos = (int) Math.ceil(1.0 * (1 + fields.length) / 8);

        for (int i = 0;i < fields.length;i++) {
            Object value = record.getColumn(i);
            if (value == null) {
                int index = (i + 1) / 8;
                int remain = (i + 1) % 8;
                // 将该列的null标志位置1
                buffer[index] &= 1 << (7 - remain);
                continue;
            }

            FieldType fieldType = fields[i].getFieldType();
            switch (fieldType) {
                case CHAR:
                    String str = (String) value;
                    byte[] chars = str.getBytes(charset);
                    int storeSize = getStoreSize(fields[i]);
                    System.arraycopy(chars, 0, buffer, pos, Math.min(chars.length, storeSize));
                    pos += storeSize;
                    break;
                case INTEGER:
                    BigInteger n = (BigInteger) value;
                    byte[] bytes = n.toByteArray();
                    int size = getStoreSize(fields[i]);

                    int fillSize = size - bytes.length;     // 计算需要先行填充字节数
                    byte fillByte = (byte) (n.signum() >= 0? 0x0 : 0xFF);   // 填充的字节
                    while (fillSize-- > 0) {
                        buffer[pos++] = fillByte;
                    }
                    // 将数据拷入
                    System.arraycopy(bytes, 0, buffer, pos, bytes.length);
                    pos += bytes.length;
                    break;
            }
        }
        buffer[0] |= 1 << 7;
        return buffer;
    }

    @Override
    public byte[] serializeSchema(Schema schema) {
        byte[] buf = new byte[DEFAULT_BUFFER_SIZE];
        int pos = 0;
        int recordSize = 0; // 单条记录的存储字节数

        // buf[0, 1]为整个的表模式的存储字节数，最后保存（小端法保存）
        // buf[2, 3]为单个记录的存储字节数，最后保存（小端法保存）
        pos += 4;
        SchemaImpl schemaImpl = (SchemaImpl) schema;
        if (schema.getStatus() == Status.NEW) {
            Arrays.fill(buf, pos, pos + 4, (byte) 0);
            pos += 4;
        } else if (schema.getStatus() == Status.UPDATE) {
            long newRecordOffset = schemaImpl.getNewRecordOffset();
            buf[pos++] = (byte) (newRecordOffset >>> 24 & 0xFF);
            buf[pos++] = (byte) (newRecordOffset >>> 16 & 0xFF);
            buf[pos++] = (byte) (newRecordOffset >>> 8 & 0xFF);
            buf[pos++] = (byte) (newRecordOffset & 0xFF);
        }

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
            int size = getStoreSize(field);
            recordSize += size;
            buf[pos++] = (byte) size;
            // 保存该字段的选项
            buf[pos++] = (byte) (((field.isPrimary() ? 1 : 0) << 2) | (field.isKey() ? 1 : 0) << 1);
        }
        // 记录中指示null字段数所需字节：一个有效位加n个字段的null指示位
        recordSize += Math.ceil(1.0 * (1 + schema.getFields().length) / 8);
        // 保存整个表模式的存储字节数
        buf[0] = (byte) (pos >>> 8 & 0xFF);
        buf[1] = (byte) (pos & 0xFF);
        // 保存单个记录存储字节数
        buf[2] = (byte) (recordSize >>> 8 & 0xFF);
        buf[3] = (byte) (recordSize & 0xFF);
        return Arrays.copyOf(buf, pos);
    }

    @Override
    public Record deserializeRecord(Schema schema, byte[] bytes) {
        if (schema == null) {
            throw new BizException("No Schema in record");
        }
        SchemaImpl schemaImpl = (SchemaImpl) schema;
        Factory factory = Global.getInstance().getFactory();
        Field[] fields = schemaImpl.getFields();
        RecordImpl recordImpl = (RecordImpl) factory.produceRecords(1, fields.length)[0];

        if ((bytes[0] >>> 7 & 0x1) == 0) {
            return null;
        }
        // 略过前导的标志位字节
        int pos = (int) Math.ceil(1.0 * (1 + fields.length) / 8);

        for(int i = 0;i < fields.length;i++) {
            int index = (i + 1) / 8;
            int remain = (i + 1) % 8;
            // 将该列的null标志位置1
            int nullFlag = (bytes[index] >> (7 - remain)) & 0x1;
            if (nullFlag != 1) {    // null标志位不为1，表明该列的值不为null
                FieldType fieldType = fields[i].getFieldType();
                int storeSize = getStoreSize(fields[i]);
                switch (fieldType) {
                    case CHAR:
                        String str = new String(bytes, pos, storeSize, charset);
                        recordImpl.setColumn(i, str);
                        break;
                    case INTEGER:
                        BigInteger n = new BigInteger(Arrays.copyOfRange(bytes, pos, pos + storeSize));
                        recordImpl.setColumn(i, n);
                        break;
                }
                pos += storeSize;
            }
        }
        recordImpl.setStatus(Status.LOAD);
        return recordImpl;
    }

    @Override
    public Schema deserializeSchema(byte[] bytes) {
        Factory factory = Global.getInstance().getFactory();
        Schema schema = factory.produceSchema();
        SchemaImpl schemaImpl = (SchemaImpl) schema;

        // pos[0, 1]为表格式的存储字节数，最后保存
        int pos = 2;
        // 读取单条记录的存储字节数
        schemaImpl.setRecordSize(Unsigned.toUnsignedByte(bytes[pos++]) << 8 | Unsigned.toUnsignedByte(bytes[pos++]));
        // 读取新纪录的偏移记录数
        schemaImpl.setNewRecordOffset(
                Unsigned.toUnsignedByte(bytes[pos++]) << 20 |
                Unsigned.toUnsignedByte(bytes[pos++]) << 16 |
                Unsigned.toUnsignedByte(bytes[pos++]) << 8 |
                Unsigned.toUnsignedByte(bytes[pos++]));

        // 读取字段个数
        int fieldSize = Unsigned.toUnsignedByte(bytes[pos++]);
        // 构造字段对象数组
        Field[] fields = new Field[fieldSize];
        for (int i = 0; i < fieldSize; i++) {
            Field field = new Field();
            field.setFieldType(FieldType.valueOf(Unsigned.toUnsignedByte(bytes[pos++])));
            int length = Unsigned.toUnsignedByte(bytes[pos++]);
            field.setName(new String(bytes, pos, length, charset));
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
