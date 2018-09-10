package storage.example;

import com.sun.xml.internal.ws.policy.privateutil.PolicyUtils;
import common.Status;
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

    /**
     * 当前可用于读写的表
     */
    private Table currentTable;
    /**
     * 当前的偏移记录数
     */
    private long currentRecordOffset;

    public Example() {
        serializer = new SerializerImpl();
    }

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
        currentTable = table;
    }

    @Override
    public void close(Table table) {
        currentTable = null;
        currentRecordOffset = 0;
    }

    @Override
    public long loadRecords(Table table) {
        if (table != currentTable) {
            throw new StorageException("Table " + table.getSchema().getTableName() + " is not open");
        }
        SchemaImpl schemaImpl = (SchemaImpl) table.getSchema();
        int recordSize = schemaImpl.getRecordSize();
        Record[] records = table.getRecords();
        String filepath = ROOT + File.separator + schemaImpl.getTableName() + TABLE_DATA_SUFFIX;
        long count = 0;
        for (int i = 0; i < records.length; i++) {
            ByteBuffer buffer = ByteBuffer.allocate(recordSize);
            int readSize = readFile(filepath, recordSize * currentRecordOffset, buffer);
            if (readSize == -1) {
                break;
            } else {
                records[i] = serializer.deserializeRecord(schemaImpl, buffer.array());
                ((RecordImpl)records[i]).setOffset(currentRecordOffset);
                currentRecordOffset++;
                count++;
            }
        }
        return count;
    }

    @Override
    public long writeRecords(Table table) {
        if (table != currentTable) {
            throw new StorageException("Table " + table.getSchema().getTableName() + " is not open");
        }
        Record[] records = table.getRecords();
        SchemaImpl schemaImpl = (SchemaImpl) table.getSchema();
        long newRecordOffset = schemaImpl.getNewRecordOffset();
        int recordSize = schemaImpl.getRecordSize();
        boolean hasNew = false;
        long count = 0;

        String filepath = ROOT + File.separator + schemaImpl.getTableName() + TABLE_DATA_SUFFIX;

        for (int i = 0; i < records.length; i++) {
            RecordImpl recordImpl = (RecordImpl) records[i];
            byte[] bytes = serializer.serializeRecord(schemaImpl, recordImpl);
            if (recordImpl.getStatus() == Status.NEW) {
                writeFile(filepath, recordSize * newRecordOffset++, ByteBuffer.wrap(bytes));
                hasNew = true;
                count++;
            } else if (recordImpl.getStatus() == Status.UPDATE || recordImpl.getStatus() == Status.DELETE) {
                writeFile(filepath, recordImpl.getOffset(), ByteBuffer.wrap(bytes));
                count++;
            }
        }
        if (hasNew) {   // 表中新增了记录
            schemaImpl.setNewRecordOffset(newRecordOffset);
            writeSchema(schemaImpl);
        }
        return count;


    }

    @Override
    public long loadRecordsById(Table table, Field[] indexs, Object[] lowerBounds, Object[] upperBounds) {
        return 0;
    }

    private int readFile(String filepath, long pos, ByteBuffer buffer) {
        RandomAccessFile raf = null;
        FileChannel channel = null;
        try {
            raf = new RandomAccessFile(filepath, "r");
            channel = raf.getChannel();
            channel.position(pos);
            int ret = channel.read(buffer);
            return ret;
        } catch (IOException e) {
            e.printStackTrace();
            throw new StorageException(e.getMessage());
        } finally {
            if (raf != null) {
                try {
                    raf.close();
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

    private void writeFile(String filepath, long pos, ByteBuffer buffer) {
        RandomAccessFile raf = null;
        FileChannel channel = null;
        try {
            raf = new RandomAccessFile(filepath, "rw");
            channel = raf.getChannel();
            channel.position(pos);
            channel.write(buffer);
        } catch (IOException e) {
            e.printStackTrace();
            throw new StorageException(e.getMessage());
        } finally {
            if (raf != null) {
                try {
                    raf.close();
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
}
