package test;

import common.Global;
import common.Status;
import storage.Persistence;
import storage.example.SchemaImpl;
import structure.*;

import java.io.*;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.Charset;
import java.util.Arrays;

public class Main {
    public static void main(String[] args) throws IOException {
//        testDeleteSchema();
//        testCreateSchema();

//        testPrintAllSchema();
        testUpdateRecord();

//        testDeleteRecord();
        testPrintAllRecord();

//        byte[] bytes = "num".getBytes(Charset.forName("utf32"));
//        String str = new String(bytes, 0, bytes.length, Charset.forName("utf32"));
//        System.out.println(str.getBytes(Charset.forName("utf32")).length);
    }

    public static void testPrintAllSchema() {
        Persistence storage = Global.getInstance().getPersistence();
        Schema[] schemas = storage.loadSchemas();
        for (Schema schema : schemas) {
            printSchema(schema);
        }
    }

    public static void testCreateSchema() {
        Persistence storage = Global.getInstance().getPersistence();
        Factory factory = Global.getInstance().getFactory();
        Schema schema = factory.produceSchema();
        schema.setTableName("hello");
        schema.setStatus(Status.NEW);
        Field field1 = new Field("num", FieldType.INTEGER, 5);
        field1.setPrimary(true);
        field1.setKey(true);
        schema.setFields(new Field[]{field1});
        storage.writeSchema(schema);
    }


    public static void testUpdateSchema() {
        /** 更新表格式，要看是否有对应记录的数据 **/
        Persistence storage = Global.getInstance().getPersistence();
        Schema[] schemas = storage.loadSchemas();
        schemas[1].setFields(new Field[]{new Field("yy", FieldType.INTEGER, 5)});
        storage.writeSchema(schemas[1]);
    }

    public static void testDeleteSchema() {
        Persistence storage = Global.getInstance().getPersistence();
        Schema[] schemas = storage.loadSchemas();
        schemas[0].setStatus(Status.DELETE);
        storage.writeSchema(schemas[0]);
    }

    public static void testPrintAllRecord() {
        Persistence storage = Global.getInstance().getPersistence();
        Schema[] schemas = storage.loadSchemas();
        Table table = new Table(schemas[0], 64);
        storage.open(table);
        storage.loadRecords(table);
        Record[] records = table.getRecords();
        for (Record record : records) {
            if (record.getStatus() == Status.LOAD) {
                printRecord(record);
            }
        }
    }

    public static void testInsertRecord() {
        Persistence storage = Global.getInstance().getPersistence();
        Schema[] schemas = storage.loadSchemas();
        Table table = new Table(schemas[0], 1);     // 插入一条数据
        Record record = table.getRecords()[0];
        record.setStatus(Status.NEW);
        record.setColumn(0, BigInteger.valueOf(123));
        storage.open(table);
        storage.writeRecords(table);
        storage.close(table);
    }

    public static void testUpdateRecord() {
        Persistence storage = Global.getInstance().getPersistence();
        Schema[] schemas = storage.loadSchemas();
        Table table = new Table(schemas[0], 1);
        storage.open(table);
        storage.loadRecords(table); // 载入一条记录
        Record[] records = table.getRecords();
        records[0].setColumn(0, new BigInteger("ff", 16));
        records[0].setStatus(Status.UPDATE);
        storage.open(table);
        storage.writeRecords(table);
    }

    public static void testDeleteRecord() {
        Persistence storage = Global.getInstance().getPersistence();
        Schema[] schemas = storage.loadSchemas();
        Table table = new Table(schemas[0], 1);
        storage.open(table);
        storage.loadRecords(table); // 载入一条记录
        storage.close(table);
        table.getRecords()[0].setStatus(Status.DELETE);
        storage.open(table);
        storage.writeRecords(table);
        storage.close(table);
    }

    private static void printRecord(Record record) {
        System.out.print("[");
        System.out.print(record.getStatus());
        for(int i = 0;i < record.getColumnSize();i++) {
            System.out.print(", " + record.getColumn(i));
        }
        System.out.println("]");
    }

    private static void printSchema(Schema schema) {
        System.out.println(schema.getTableName() + ":");
        System.out.println("\t" + schema.getStatus());
        System.out.println("\tFields:");
        Field[] fields = schema.getFields();
        for (Field field : fields) {
            System.out.print("\t\t[");
            System.out.print(field.getName()+ ", " + field.getFieldType() + ", " + field.getLength());
            System.out.println("]");
        }
    }
}
