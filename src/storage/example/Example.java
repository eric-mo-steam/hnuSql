package storage.example;

import exception.StorageException;
import javafx.scene.control.Tab;
import number.Unsigned;
import storage.Persistence;
import storage.Serializer;
import structure.Field;
import structure.Record;
import structure.Schema;
import structure.Table;

import java.io.*;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.channels.Channel;
import java.nio.channels.FileChannel;
import java.util.Arrays;

public class Example implements Persistence {
    /**
     * 序列化器
     */
    private Serializer serializer;
    /**
     * 文件魔术头
     */
    private static final byte MAGIC = 53;
    /**
     * 文件版本
     */
    private static final byte VERSION = 1;
    /**
     * 数据根目录
     */
    private static final String ROOT = "HNUSQLData";
    /**
     * 模式文件的后缀名
     */
    private static final String SCHEMA_SUFFIX = ".frm";
    /**
     * 表数据的后缀名
     */
    private static final String TABLE_DATA_SUFFIX = ".MYD";


    public Example() {
        serializer = new SerializerImpl();
    }

    /**
     * 未做魔术字、版本号、存储大小的检验
     */
//    public Schema[] loadSchemas() {
//        try {
//            File root = new File(ROOT);
//            if (root.exists() && root.isDirectory()) {
//                // 列出数据根目录下，所有的表模式文件
//                File[] files = root.listFiles(new FilenameFilter() {
//                    @Override
//                    public boolean accept(File dir, String name) {
//                        return name.endsWith(SCHEMA_SUFFIX);
//                    }
//                });
//                Schema[] schemas = new Schema[files.length];
//                for (int i = 0;i < files.length;i++) {
//                    schemas[i] = new Schema();
//
//                    FileInputStream in = new FileInputStream(files[i]);
//                    byte[] buf = new byte[BUF_SIZE];
//                    in.read(buf, 0, in.available());
//                    int pos = 4;
//                    schemas[i].setRecordSize(Unsigned.toUnsignedByte(buf[pos++]) | Unsigned.toUnsignedByte(buf[pos++]) << 8);
//                    schemas[i].setNewRecordOffset(Unsigned.toUnsignedByte(buf[pos++]) | Unsigned.toUnsignedByte(buf[pos++]) << 8);
//                    // 读取字段个数
//                    int fieldSize = Unsigned.toUnsignedByte(buf[pos++]);
//                    // 构造字段对象
//                    Field[] fields = new Field[fieldSize];
//                    for(int j = 0;j < fieldSize;j++) {
//                        Field field = new Field();
//                        field.setFieldType(FieldType.valueOf(Unsigned.toUnsignedByte(buf[pos++])));
//                        int length = Unsigned.toUnsignedByte(buf[pos++]);
//                        field.setName(new String(buf, pos, length));
//                        pos += length;
//                        field.setLength(Unsigned.toUnsignedByte(buf[pos++]));
//                        pos++;
//                        field.setPrimary((buf[pos] >> 1 & 0x1) == 1);
//                        field.setKey((buf[pos] & 0x1) == 1);
//                        pos++;
//                        fields[j] = field;
//                    }
//                    String fileName = files[i].getName();
//                    schemas[i].setTableName(fileName.substring(0, fileName.lastIndexOf(SCHEMA_SUFFIX)));
//                    schemas[i].setFields(fields);
//                    in.close();
//                }
//                return schemas;
//            }
//        } catch (FileNotFoundException e) {
//            e.printStackTrace();
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
//        return null;
//    }

//    public long loadRecords(Record[] buffer, long offset, long size) {
//        long current = offset;
//        if (buffer.length == 0) {
//            return 0;
//        }
//
//        Schema schema = buffer[0].getSchema();
//        if (schema == null) {
//            throw new BizException("No Schema in record");
//        }
//
//        ByteBuffer byteBuffer = ByteBuffer.allocate(schema.getRecordSize());
//        String filepath = ROOT + File.separator + schema.getTableName() + TABLE_DATA_SUFFIX;
//
//        FileInputStream fin = null;
//        try {
//            fin = new FileInputStream(filepath);
//            FileChannel channel = fin.getChannel();
//            for(int i = 0;i < size;i++) {
//                channel.read(byteBuffer);
//                buffer[0] = toRecord(byteBuffer.array(), schema);
//            }
//        } catch (IOException e) {
//            e.printStackTrace();
//            throw new StorageException(e.getMessage());
//        }
//
//        return 0;
//    }


//    public long writeRecords(Record[] buffer) {
//        if (buffer.length == 0) {
//            return 0;
//        }
//
//        Schema schema = buffer[0].getSchema();
//        if (schema == null) {
//            throw new BizException("No Schema in record");
//        }
//
//        RandomAccessFile raf = null;
//        FileChannel channel = null;
//        String filepath = ROOT + File.separator + schema.getTableName() + TABLE_DATA_SUFFIX;
//        int count = 0;
//        try {
//            File file = new File(filepath);
//            raf = new RandomAccessFile(file, "rw");
//            channel = raf.getChannel();
//            for (int i = 0;i < buffer.length;i++) {
//                Record record = buffer[i];
//                byte status = record.getStatus();
//                if (status == Record.CREATE) {
//                    channel.position(schema.getNewRecordOffset() * schema.getRecordSize());
//                    ByteBuffer byteBuffer = ByteBuffer.wrap(toBytes(record));
//                    channel.write(byteBuffer);
//                } else if(status == Record.UPDATE) {
//                    channel.position(record.getOffset() * schema.getRecordSize());
//                    ByteBuffer byteBuffer = ByteBuffer.wrap(toBytes(record));
//                    channel.write(byteBuffer);
//                } else if (status == Record.DELETE) {
//                    channel.position(record.getOffset() * schema.getRecordSize());
//                    ByteBuffer byteBuffer = ByteBuffer.wrap(toBytes(record));
//                    channel.write(byteBuffer);
//                }
//                count++;
//            }
//        } catch (IOException e) {
//            e.printStackTrace();
//            throw new StorageException(e.getMessage());
//        }
//        return count;
//    }


//    public Object loadRecordsById(Record[] buffer, Field[] indexs, Object[] lowerBounds, Object[] upperBounds, Object offset, long size) {
//        return null;
//    }


//    public boolean writeSchema(Schema schema) {
//        try {
//            byte[] buf = new byte[BUF_SIZE];
//            int pos = 0;
//            int recordSize = 0;
//            buf[pos++] = MAGIC;
//            buf[pos++] = VERSION;
//            // buf[2, 3]为整个的表模式的存储字节数，最后保存（小端法保存）
//            // buf[4, 5]为单个记录的存储字节数，最后保存（小端法保存）
//            pos += 4;
//            // buf[6, 7]为新记录的偏移记录数，初始化为0
//            buf[pos++] = 0;
//            buf[pos++] = 0;
//            // 字段数
//            buf[pos++] = (byte) schema.getFields().length;
//            for (Field field : schema.getFields()) {
//                // 存储字段类型
//                buf[pos++] = (byte) field.getFieldType().getValue();
//                byte[] chars = field.getName().getBytes(charset);
//                // 存储字段名长度
//                buf[pos++] = (byte) chars.length;
//                // 存储字段名
//                System.arraycopy(chars, 0, buf, pos, chars.length);
//                // 设置此时的pos位置
//                pos += chars.length;
//                // 存储字段的字面值长度
//                buf[pos++] = (byte) field.getLength();
//                // 存储字段的实际存储字节数
//                int size = getSize(field.getFieldType(), field.getLength());
//                recordSize += size;
//                buf[pos++] = (byte) size;
//                // 保存该字段的选项
//                buf[pos++] = (byte) (((field.isPrimary() ? 1 : 0) << 2) | (field.isKey() ? 1 : 0) << 1) ;
//            }
//            // 一个有效位加n个字段的null指示位
//            recordSize += Math.ceil(1.0 * (1 + schema.getFields().length) / 8);
//            // 保存整个表模式的存储字节数
//            buf[2] = (byte) (pos & 0xFF);
//            buf[3] = (byte) (pos & 0xFF00);
//            // 保存单个记录存储字节数
//            buf[4] = (byte)(recordSize & 0xFF);
//            buf[5] = (byte)(recordSize & 0xFF00);
//
//            FileOutputStream out = new FileOutputStream(ROOT + File.separator + schema.getTableName() + SCHEMA_SUFFIX);
//            out.write(buf, 0, pos);
//            out.close();
//            File file = new File(ROOT + File.separator + schema.getTableName() + TABLE_DATA_SUFFIX);
//            file.createNewFile();
//            return true;
//        } catch (IOException  e) {
//            e.printStackTrace();
//            throw new StorageException(e.getMessage());
//        }
//    }


//    private byte[] toBytes(Record record) {
//        Schema schema = record.getSchema();
//        if (schema == null) {
//            throw new BizException("No Schema in record");
//        }
//
//        byte[] buf = new byte[schema.getRecordSize()];
//        if (record.getStatus() == Record.DELETE) {
//            return buf;
//        }
//
//        Field[] fields = schema.getFields();
//        // 先略过前导字节
//        int pos = (int) Math.ceil(1.0 * (1 + fields.length) / 8);
//
//        for (int i = 0;i < fields.length;i++) {
//            Object value = record.getColumn(i);
//            if (value == null) {
//                int index = (i + 1) / 8;
//                int remain = (i + 1) % 8;
//                // 将该列的null标志为置1
//                buf[index] &= 1 << (7 - remain);
//                continue;
//            }
//
//            FieldType fieldType = fields[i].getFieldType();
//            switch (fieldType) {
//                case CHAR:
//                    String str = (String) value;
//                    byte[] chars = str.getBytes(charset);
//                    System.arraycopy(chars, 0, buf, pos, chars.length);
//                    pos += chars.length;
//                    break;
//                case INTEGER:
//                    BigInteger n = (BigInteger) value;
//                    byte[] bytes = n.toByteArray();
//                    int size = getSize(fieldType, fields[i].getLength());
//                    // 填充字节数
//                    int fillSize = size - bytes.length;
//                    // 填充的字节
//                    byte fileByte = (byte) (n.signum() >= 0? 0x0 : 0xFF);
//                    while (fillSize-- > 0) {
//                        buf[pos++] = fileByte;
//                    }
//                    // 拷贝值
//                    System.arraycopy(bytes, 0, buf, pos, bytes.length);
//                    break;
//            }
//        }
//        buf[0] &= 1 << 7;
//        return buf;
//    }
    @Override
    public boolean writeSchema(Schema schema) {
        FileOutputStream fout = null;
        FileChannel channel = null;
        try {
            byte[] schemaBytes = serializer.serializeSchema(schema);
            byte[] buf = new byte[schemaBytes.length + 2];
            int pos = 0;
            buf[pos++] = MAGIC;
            buf[pos++] = VERSION;
            System.arraycopy(schemaBytes, 0, buf, pos, schemaBytes.length);

            // 写入表格式文件
            fout = new FileOutputStream(ROOT + File.separator + schema.getTableName() + SCHEMA_SUFFIX);
            channel = fout.getChannel();
            channel.write(ByteBuffer.wrap(buf));

            // 创建表数据文件
            File file = new File(ROOT + File.separator + schema.getTableName() + TABLE_DATA_SUFFIX);
            file.createNewFile();
            return true;
        } catch (IOException e) {
            e.printStackTrace();
            throw new StorageException(e.getMessage());
        } finally {
            if (fout != null) {
                try {
                    fout.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            if (channel != null) {
                try {
                    channel.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    @Override
    public Schema[] loadSchemas() {
        FileInputStream fin = null;
        FileChannel channel = null;
        try {
            File root = new File(ROOT);
            if (root.exists() && root.isDirectory()) {
                // 列出数据根目录下，所有的表模式文件
                File[] files = root.listFiles(new FilenameFilter() {
                    @Override
                    public boolean accept(File dir, String name) {
                        return name.endsWith(SCHEMA_SUFFIX);
                    }
                });
                Schema[] schemas = new Schema[files.length];
                for (int i = 0; i < files.length; i++) {
                    fin = new FileInputStream(files[i]);
                    channel = fin.getChannel();

                    int size = fin.available();
                    byte[] buf = new byte[size];
                    channel.read(ByteBuffer.wrap(buf));

                    // buf[0, 1]是魔术字和版本号
                    schemas[i] = serializer.deserializeSchema(Arrays.copyOfRange(buf, 2, size));

                    String fileName = files[i].getName();
                    // 保存表名
                    schemas[i].setTableName(fileName.substring(0, fileName.lastIndexOf(SCHEMA_SUFFIX)));
                }
                return schemas;
            }
        } catch (IOException e) {
            e.printStackTrace();
            throw new StorageException(e.getMessage());
        } finally {
            if (fin != null) {
                try {
                    fin.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            if (channel != null) {
                try {
                    channel.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
        return null;
    }

    @Override
    public void open(Table table) {

    }

    @Override
    public void close(Table table) {

    }

    @Override
    public long loadRecords(Table table) {
        return 0;
    }

    @Override
    public long writeRecords(Table table) {
        return 0;
    }

    @Override
    public long loadRecordsById(Table table, Field[] indexs, Object[] lowerBounds, Object[] upperBounds) {
        return 0;
    }

}
