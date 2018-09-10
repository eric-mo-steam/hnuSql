package test;

import common.Global;
import common.Status;
import storage.Persistence;
import storage.example.SchemaImpl;
import structure.Field;
import structure.FieldType;
import structure.Table;
import structure.Schema;

import java.io.*;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.Charset;
import java.util.Arrays;

public class Main {
    public static void main(String[] args) {
        testExample();

    }

    public static void testArraysCopy() {
        byte[] bytes = new byte[10];
        bytes[9] = 1;
        bytes[8] = 1;
        byte[] bytes2 = Arrays.copyOfRange(bytes, 5, 10);
        System.out.println(bytes2);
    }

    public static void testEnum() {
        System.out.println(Status.valueOf(""));
    }

    public static void testRecordGroup() {
        Schema schema = null;
        Table table = new Table(schema, 1);
    }

    public static void testNIO() {
        FileInputStream fout = null;
        try {
            fout = new FileInputStream("NIO-test.txt");
            FileChannel channel = fout.getChannel();
            channel.position(2);
            ByteBuffer rBuffer = ByteBuffer.allocate(1);
            channel.read(rBuffer);
            System.out.write(rBuffer.array());
//            int i = 9;
//            ByteBuffer byteBuffer = ByteBuffer.wrap("1".getBytes());
//            channel.position(0);
//            channel.write(byteBuffer);
//            channel.read(rBuffer);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void testGetBigIntegerBytes() {
        BigInteger bi = new BigInteger("256");
        System.out.println(bi.setBit(31));
        System.out.println(0);
        System.out.print("[");
        for (byte b : bi.toByteArray()) {
            System.out.print(b + ", ");
        }
        System.out.println("]");
    }

    public static void testGetExponent() {
        System.out.println(Math.getExponent(Math.pow(10, 2) - 1) + 2);
    }

    public static void testBigInteger() {
        byte[] b = new byte[2];
        b[0] = (byte)128;
        b[1] = 0;
        BigInteger bigInteger = new BigInteger(b);
        System.out.println(bigInteger.toString());
    }

    public static void testEncode() {
        Charset charset = Charset.forName("utf32");
        System.out.println("a".getBytes(charset).length);
    }

    public static void testExample() {
        Field field1 = new Field();
        field1.setFieldType(FieldType.INTEGER);
        field1.setName("id");
        field1.setLength(8);
        Field field2 = new Field();
        field2.setFieldType(FieldType.CHAR);
        field2.setLength(20);
        field2.setName("name");
        Schema schema = new SchemaImpl();
        schema.setFields(new Field[] {field1, field2});
        schema.setTableName("test");

        Persistence storage = Global.getInstance().getPersistence();

        boolean b = storage.writeSchema(schema);
//        storage.loadSchemas();
    }
}
