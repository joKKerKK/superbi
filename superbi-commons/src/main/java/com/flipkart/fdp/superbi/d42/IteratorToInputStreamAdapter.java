package com.flipkart.fdp.superbi.d42;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.util.Iterator;
import java.util.Objects;

public class IteratorToInputStreamAdapter extends InputStream {
    private final Iterator<byte[]> iterator;
    private final InternalBuffer internalBuffer;
    private final byte[] ROW_DELIMETER;
    private final byte[] EMPTY_BYTE_ARRAY = new byte[0];

    private boolean isFirstRow = true;

    private class InternalBuffer {
        private final byte[] buf;
        private int nextWritePos = 0;
        private int nextReadPos = 0;

        InternalBuffer(int capacity) {
            buf = new byte[capacity];
        }

        int length() {
            return nextWritePos - nextReadPos;
        }

        void reset() {
            nextWritePos = nextReadPos = 0;
        }

        void resetIfNeeded() {
            if(length() == 0) {
                reset();
            }
        }

        void append(byte[] src, int srcOffset, int length) {
            System.arraycopy(src, srcOffset, buf, nextWritePos, length);
            nextWritePos += length;
        }

        int copyTo(byte[] dest, int destOffset, int length) {
            int bytesToCopy = Math.min(length(), length);

            if (bytesToCopy > 0) { //Just in case buffer length was 0
                System.arraycopy(buf, nextReadPos, dest, destOffset, bytesToCopy);
                nextReadPos += bytesToCopy;

                // There is a possibility of needing a reset
                resetIfNeeded();
            }
            return bytesToCopy;
        }
    }

    public IteratorToInputStreamAdapter(Iterator<byte[]> iterator, int maxRowSize, String rowDelimeter) {
        Objects.requireNonNull(iterator, "Iterator can not be null");
        if(maxRowSize <= 0 ) {
            throw new IllegalArgumentException("maxRowSize has to be greater than 0");
        }
        Objects.requireNonNull(rowDelimeter, "rowDelimeter can not be null");

        this.iterator = iterator;
        this.internalBuffer = new InternalBuffer(maxRowSize);
        ROW_DELIMETER = rowDelimeter.getBytes();
    }

    @Override
    public int read() throws IOException {
        byte[] bytes = new byte[1];
        int bytesRead = read(bytes, 0, 1);
        if(bytesRead == -1) {
            return -1; //End of Stream
        }
        return bytes[0];
    }

    @Override
    public void close() throws IOException {
        if(iterator instanceof Closeable);{
            ((Closeable)iterator).close();
        }
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        int originalLen = len;

        int bytesCopied = internalBuffer.copyTo(b, off, len);
        off += bytesCopied;
        len -= bytesCopied;

        byte[] nextElementBytes = EMPTY_BYTE_ARRAY; //Just to avoid null check
        while(len > 0 && iterator.hasNext()) {
            if(isFirstRow) {
                nextElementBytes = iterator.next();
                isFirstRow = false; //For next row onwards
            }
            else {
                byte[] tempNext = iterator.next();
                nextElementBytes = new byte[ROW_DELIMETER.length + tempNext.length];
                System.arraycopy(ROW_DELIMETER, 0, nextElementBytes, 0, ROW_DELIMETER.length);
                System.arraycopy(tempNext, 0, nextElementBytes, ROW_DELIMETER.length, tempNext.length);
            }
            bytesCopied = Math.min(nextElementBytes.length, len);
            System.arraycopy(nextElementBytes, 0, b, off, bytesCopied);
            off = off + bytesCopied;
            len = len - bytesCopied;
        }

        //if we could not copy whole of nextElement, let's buffer
        if(bytesCopied < nextElementBytes.length) {
            int bytesPending = nextElementBytes.length - bytesCopied;
            internalBuffer.append(nextElementBytes, bytesCopied, bytesPending);
        }

        if(len == originalLen) {
            return -1; //we could not read a single data
        }
        return originalLen - len;
    }
}
