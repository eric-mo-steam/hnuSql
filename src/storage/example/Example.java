package storage.example;

import common.Status;
import exception.StorageException;
import storage.Persistence;
import storage.Serializer;
import structure.*;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.*;
import java.util.stream.Collectors;

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
     * 索引数据的后缀名
     */
    private static final String INDEX_DATA_SUFFIX = ".INDEX";


    /**
     * 当前可用于读写的表
     */
    private Table currentTable;
    /**
     * 当前的偏移记录数
     */
    private long currentRecordOffset;

    /**
     * 通过索引读取记录数量
     */
    private long count = 0;


    /**
     * 记录当前索引对象
     */
    private HashMap<String, FieldIndex> indexMap = new HashMap<>();


    public Example() {
        serializer = new SerializerImpl();
    }

    @Override
    public boolean writeSchema(Schema schema) {
        if (schema.getStatus() != Status.UPDATE &&
                schema.getStatus() != Status.NEW &&
                schema.getStatus() != Status.DELETE) {
            return false;
        }
        if (schema.getStatus() == Status.DELETE) {
            String filepath = ROOT + File.separator + schema.getTableName() + SCHEMA_SUFFIX;
            File file = new File(filepath);
            file.delete();

            filepath = ROOT + File.separator + schema.getTableName() + TABLE_DATA_SUFFIX;
            file = new File(filepath);
            file.delete();
            return true;
        }
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
                    // 设置表格式的状态
                    schemas[i].setStatus(Status.LOAD);
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
        int count = 0;
        while (count < records.length) {
            ByteBuffer buffer = ByteBuffer.allocate(recordSize);
            int readSize = readFile(filepath, recordSize * currentRecordOffset, buffer);
            if (readSize == -1) {
                break;
            } else {
                RecordImpl recordImpl = (RecordImpl) serializer.deserializeRecord(schemaImpl, buffer.array());
                if (recordImpl != null) {
                    records[count] = recordImpl;
                    recordImpl.setOffset(currentRecordOffset);
                    count++;
                }
                currentRecordOffset++;
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
            } else if (recordImpl.getStatus() == Status.UPDATE) {
                writeFile(filepath, recordSize * recordImpl.getOffset(), ByteBuffer.wrap(bytes));
                count++;
            } else if (recordImpl.getStatus() == Status.DELETE) {
                writeFile(filepath, recordSize * recordImpl.getOffset(), ByteBuffer.wrap(bytes));
                count++;
            }
        }
        if (hasNew) {   // 表中新增了记录
            schemaImpl.setNewRecordOffset(newRecordOffset);
            schemaImpl.setStatus(Status.UPDATE);
            writeSchema(schemaImpl);
        }
        /** index注释　**/
//        writeIndex(table);
        return count;
    }

    /**
     * 将字段索引写进磁盘
     *
     * @param indexs
     */
    public void writeFieldIndex(FieldIndex indexs) {

        String filepath = ROOT + File.separator + indexs.fieldName + INDEX_DATA_SUFFIX;
        try {
            ObjectOutputStream out = new ObjectOutputStream(new FileOutputStream(filepath));
            out.writeObject(indexs);
            out.flush();
            out.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    /**
     * 获取字段索引
     *
     * @param field
     * @return
     */
    public FieldIndex getIndexs(Field field) {

        //先从内存中读取，没有再读取磁盘
        if (indexMap.get(field.getName()) != null) {
            return indexMap.get(field.getName());
        }

        String filepath = ROOT + File.separator + field.getName() + INDEX_DATA_SUFFIX;
        FileInputStream inputStream;
        try {
            inputStream = new FileInputStream(filepath);
            byte[] bytes = new byte[inputStream.available()];
            new DataInputStream(inputStream).readFully(bytes);
            ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
            ObjectInputStream ois;
            FieldIndex indexs = null;
            try {
                ois = new ObjectInputStream(bais);
                bais.close();
                indexs = (FieldIndex) ois.readObject();

                //加载进内存
                indexMap.put(field.getName(), indexs);

                return indexs;
            } catch (Exception e) {
                e.printStackTrace();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }


    /**
     * 保存表中具有索引的字段
     *
     * @param table
     */
    public void writeIndex(Table table) {

        Field[] fields = table.getSchema().getFields();
        Record[] records = table.getRecords();
        for (int i = 0; i < fields.length; i++) {
            if (fields[i].isKey() || fields[i].isPrimary()) {
                FieldIndex indexs = getIndexs(fields[i]);

                for (Record record : records) {
                    Index index = new Index(record.getColumn(i), ((RecordImpl) record).getOffset());
                    indexs.indexs.add(index);
                }
                //将索引写进磁盘
                this.writeFieldIndex(indexs);

                indexMap.put(fields[i].getName(), indexs);
            }
        }

    }


    @Override
    public long loadRecordsById(Table table, Field index, Object lowerBound, Object upperBound) {
        //TODO

        FieldIndex fieldIndex = this.getIndexs(index);

        if (fieldIndex == null) {
            return 0;
        }

        if (index.getFieldType() == FieldType.INTEGER) {
            if ((int) lowerBound > (int) upperBound) {
                return 0;
            } else if ((int) lowerBound == (int) upperBound) {
                for (Index i : fieldIndex.indexs) {
                    if (i.equals(lowerBound)) {
                        long offset = i.offset;
                        ByteBuffer bytes = ByteBuffer.allocate(((SchemaImpl) table.getSchema()).getRecordSize());

                        SchemaImpl schemaImpl = (SchemaImpl) table.getSchema();

                        String filepath = ROOT + File.separator + schemaImpl.getTableName() + TABLE_DATA_SUFFIX;

                        int isExist = this.readFile(filepath, offset * schemaImpl.getRecordSize(), bytes);
                        if (isExist != -1) {
                            table.getRecords()[0] = this.serializer.deserializeRecord(schemaImpl, bytes.array());
                            ((RecordImpl) table.getRecords()[0]).setOffset(offset);
                        }
                        return 1;
                    }
                }
            } else {

                ArrayList<Index> list = fieldIndex.indexs;
                list.sort(new Comparator() {
                    @Override
                    public int compare(Object o1, Object o2) {

                        if ((int) ((Index) o1).value > (int) ((Index) o2).value) {
                            return 1;
                        } else {
                            return 0;
                        }
                    }
                });

                Object[] values = list.stream().map(e -> (int) e.value).collect(Collectors.toList()).toArray();
                int i = Arrays.binarySearch(values, lowerBound);
                int j = Arrays.binarySearch(values, upperBound);

                int size = table.getRecords().length;

                if (i <= 0 && j >= list.size()) {
                    long c = 0;
                    if (j > size) {
                        long k = count;
                        for (int m = 0; k < list.size() && k < count + size; k++, m++) {
                            long offset = list.get((int) k).offset;
                            ByteBuffer bytes = ByteBuffer.allocate(((SchemaImpl) table.getSchema()).getRecordSize());

                            SchemaImpl schemaImpl = (SchemaImpl) table.getSchema();

                            String filepath = ROOT + File.separator + schemaImpl.getTableName() + TABLE_DATA_SUFFIX;

                            int isExist = this.readFile(filepath, offset * schemaImpl.getRecordSize(), bytes);
                            if (isExist != -1) {
                                table.getRecords()[m] = this.serializer.deserializeRecord(schemaImpl, bytes.array());
                                ((RecordImpl) table.getRecords()[m]).setOffset(offset);
                            }
                            c++;
                        }
                        count += k;
                        return c;
                    } else {
                        c = 0;
                        for (int k = 0; k < list.size(); k++) {
                            long offset = list.get(k).offset;
                            ByteBuffer bytes = ByteBuffer.allocate(((SchemaImpl) table.getSchema()).getRecordSize());
                            SchemaImpl schemaImpl = (SchemaImpl) table.getSchema();
                            String filepath = ROOT + File.separator + schemaImpl.getTableName() + TABLE_DATA_SUFFIX;

                            int isExist = this.readFile(filepath, offset * schemaImpl.getRecordSize(), bytes);
                            if (isExist != -1) {
                                table.getRecords()[k] = this.serializer.deserializeRecord(schemaImpl, bytes.array());
                                ((RecordImpl) table.getRecords()[k]).setOffset(offset);
                            }
                            c++;
                        }
                        return c;
                    }


                } else if (j >= list.size()) {
                    int c = 0;
                    if (list.size() - i > size) {
                        long k = count;
                        for (int m = 0; k < list.size() && k < count + size; k++, m++) {
                            long offset = list.get((int) k).offset;
                            ByteBuffer bytes = ByteBuffer.allocate(((SchemaImpl) table.getSchema()).getRecordSize());

                            SchemaImpl schemaImpl = (SchemaImpl) table.getSchema();

                            String filepath = ROOT + File.separator + schemaImpl.getTableName() + TABLE_DATA_SUFFIX;

                            int isExist = this.readFile(filepath, offset * schemaImpl.getRecordSize(), bytes);
                            if (isExist != -1) {
                                table.getRecords()[m] = this.serializer.deserializeRecord(schemaImpl, bytes.array());
                                ((RecordImpl) table.getRecords()[m]).setOffset(offset);
                            }
                            c++;
                        }
                        count += k;
                        return c;

                    } else {
                        for (int k = i; k < list.size(); k++) {
                            long offset = list.get(k).offset;
                            ByteBuffer bytes = ByteBuffer.allocate(((SchemaImpl) table.getSchema()).getRecordSize());
                            SchemaImpl schemaImpl = (SchemaImpl) table.getSchema();
                            String filepath = ROOT + File.separator + schemaImpl.getTableName() + TABLE_DATA_SUFFIX;

                            int isExist = this.readFile(filepath, offset * schemaImpl.getRecordSize(), bytes);
                            if (isExist != -1) {
                                table.getRecords()[k] = this.serializer.deserializeRecord(schemaImpl, bytes.array());
                                ((RecordImpl) table.getRecords()[k]).setOffset(offset);
                            }
                            c++;
                        }
                        return c;
                    }


                } else if (i <= 0) {
                    int c = 0;
                    if (j > size) {
                        long k = count;
                        for (int m = 0; k < j && k < count + size; k++, m++) {
                            long offset = list.get((int) k).offset;
                            ByteBuffer bytes = ByteBuffer.allocate(((SchemaImpl) table.getSchema()).getRecordSize());

                            SchemaImpl schemaImpl = (SchemaImpl) table.getSchema();

                            String filepath = ROOT + File.separator + schemaImpl.getTableName() + TABLE_DATA_SUFFIX;

                            int isExist = this.readFile(filepath, offset * schemaImpl.getRecordSize(), bytes);
                            if (isExist != -1) {
                                table.getRecords()[m] = this.serializer.deserializeRecord(schemaImpl, bytes.array());
                                ((RecordImpl) table.getRecords()[m]).setOffset(offset);
                            }
                            c++;
                        }
                        count += k;
                        return c;

                    } else {
                        c = 0;
                        for (int k = 0; k < j; k++) {
                            long offset = list.get(k).offset;
                            ByteBuffer bytes = ByteBuffer.allocate(((SchemaImpl) table.getSchema()).getRecordSize());
                            SchemaImpl schemaImpl = (SchemaImpl) table.getSchema();
                            String filepath = ROOT + File.separator + schemaImpl.getTableName() + TABLE_DATA_SUFFIX;

                            int isExist = this.readFile(filepath, offset * schemaImpl.getRecordSize(), bytes);
                            if (isExist != -1) {
                                table.getRecords()[k] = this.serializer.deserializeRecord(schemaImpl, bytes.array());
                                ((RecordImpl) table.getRecords()[k]).setOffset(offset);
                            }
                            c++;
                        }
                        return c;

                    }

                } else {
                    int c = 0;
                    if (j - i > size) {
                        long k = count + i;
                        for (int m = 0; k < j && k < count + size; k++, m++) {
                            long offset = list.get((int) k).offset;
                            ByteBuffer bytes = ByteBuffer.allocate(((SchemaImpl) table.getSchema()).getRecordSize());

                            SchemaImpl schemaImpl = (SchemaImpl) table.getSchema();

                            String filepath = ROOT + File.separator + schemaImpl.getTableName() + TABLE_DATA_SUFFIX;

                            int isExist = this.readFile(filepath, offset * schemaImpl.getRecordSize(), bytes);
                            if (isExist != -1) {
                                table.getRecords()[m] = this.serializer.deserializeRecord(schemaImpl, bytes.array());
                                ((RecordImpl) table.getRecords()[m]).setOffset(offset);
                            }
                            c++;
                        }
                        count += k;
                        return c;

                    } else {

                        c = 0;
                        for (int k = i; k < j; k++) {
                            long offset = list.get(k).offset;
                            ByteBuffer bytes = ByteBuffer.allocate(((SchemaImpl) table.getSchema()).getRecordSize());
                            SchemaImpl schemaImpl = (SchemaImpl) table.getSchema();
                            String filepath = ROOT + File.separator + schemaImpl.getTableName() + TABLE_DATA_SUFFIX;

                            int isExist = this.readFile(filepath, offset * schemaImpl.getRecordSize(), bytes);
                            if (isExist != -1) {
                                table.getRecords()[k] = this.serializer.deserializeRecord(schemaImpl, bytes.array());
                                ((RecordImpl) table.getRecords()[k]).setOffset(offset);
                            }
                            c++;
                        }
                        return c;
                    }

                }
            }

        } else {

            for (Index i : fieldIndex.indexs) {
                if (i.equals(lowerBound)) {
                    long offset = i.offset;
                    ByteBuffer bytes = ByteBuffer.allocate(((SchemaImpl) table.getSchema()).getRecordSize());

                    SchemaImpl schemaImpl = (SchemaImpl) table.getSchema();

                    String filepath = ROOT + File.separator + schemaImpl.getTableName() + TABLE_DATA_SUFFIX;

                    int isExist = this.readFile(filepath, offset * schemaImpl.getRecordSize(), bytes);
                    if (isExist != -1) {
                        table.getRecords()[0] = this.serializer.deserializeRecord(schemaImpl, bytes.array());
                        ((RecordImpl) table.getRecords()[0]).setOffset(offset);
                    }
                    return 1;
                }
            }
        }

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
            if (channel != null) {
                try {
                    channel.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            if (raf != null) {
                try {
                    raf.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }

        }
    }
}

/**
 * 索引
 */
class Index implements Serializable {
    Object value;
    long offset;

    public Index(Object value, long offset) {
        this.value = value;
        this.offset = offset;
    }
}

/**
 * 字段对应的所有索引
 */
class FieldIndex implements Serializable {

    ArrayList<Index> indexs;

    String fieldName;

    public FieldIndex(ArrayList<Index> indexs, String fieldName) {
        this.indexs = indexs;
        this.fieldName = fieldName;
    }
}
