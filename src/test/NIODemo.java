package test;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;

public class NIODemo {
    public static void main(String[] args) {
        readNIO();
    }

    public static void writeNIO() {

    }

    public static void readNIO() {
        FileInputStream fin = null;
        FileChannel channel = null;
        try {
            fin = new FileInputStream("NIO-test.txt");
            channel = fin.getChannel();
            channel.position(5);

            ByteBuffer buf = ByteBuffer.allocate(5);
            channel.read(buf);
            buf.flip();
            System.out.write(buf.array(), 0, buf.limit());
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
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

    }

    public static void readAndWriteNIO() {
        RandomAccessFile raf = null;
        FileChannel channel = null;
        try {
            raf = new RandomAccessFile("NIO-test.txt", "rw");
            channel = raf.getChannel();
            // 开启一个非共享锁
            FileLock lock = channel.lock();
            channel.position();


            lock.release();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
