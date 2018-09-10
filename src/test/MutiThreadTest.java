package test;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

public class MutiThreadTest {
    public static void main(String[] args) throws IOException {

        Thread thread = new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    RandomAccessFile raf = new RandomAccessFile("NIO-test.txt", "rw");
                    FileChannel fileChannel = raf.getChannel();
//                    FileLock fileLock = fileChannel.lock();
                    fileChannel.write(ByteBuffer.wrap("9".getBytes()));
                    try {
                        Thread.sleep(500);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
//                    fileLock.release();
                } catch (IOException e) {
                    e.printStackTrace();
                }

            }
        });
        thread.start();

        RandomAccessFile raf = new RandomAccessFile("NIO-test.txt", "rw");
        FileChannel fileChannel = raf.getChannel();
//        FileLock fileLock = fileChannel.lock();
//        fileChannel.lock();
        fileChannel.write(ByteBuffer.wrap("1".getBytes()));
        try {
            Thread.sleep(500);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
//        fileLock.release();
    }
}
