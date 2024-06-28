package com.flipkart.fdp.superbi.d42;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Objects;
import lombok.SneakyThrows;

public class ChunkedInputStreamIterator implements Iterator<ByteArrayInputStream>, Iterable<ByteArrayInputStream> {

    private final InputStream inputStream;
    private final int ADJUSTED_MAX_SIZE_PER_CHUNK; //It is adjusted to accommodate look ahead byte
    private Integer lookAheadByte;

    public ChunkedInputStreamIterator(InputStream inputStream, int maxSizePerChunk) {
        Objects.requireNonNull(inputStream, "InputStream can not be null");
        if(maxSizePerChunk <= 0){
            throw new IllegalArgumentException("Chunk size has to be greater than 0");
        }
        this.inputStream = inputStream;
        this.ADJUSTED_MAX_SIZE_PER_CHUNK = maxSizePerChunk - 1; //Everytime we will have a lookAheadByte to be prefixed
        lookAhead();
    }

    @Override
    public boolean hasNext() {
        //Can't call lookAhead() here as it might cause side effects.
        return lookAheadByte != -1; //Will return false if we ever read less than maxBytes
    }

    @SneakyThrows
    @Override
    public ByteArrayInputStream next() {
        if(!hasNext()) {
            throw new NoSuchElementException();
        }
        //Adjusting the lookAheadByte in below lines
        byte[] nextBytes = new byte[ADJUSTED_MAX_SIZE_PER_CHUNK +1]; //+1 for lookAheadByte
        nextBytes[0] = lookAheadByte.byteValue();
        int totalBytesInChunk = 1;

        int lastReadBytes = inputStream.read(nextBytes, 1, ADJUSTED_MAX_SIZE_PER_CHUNK); //Starting offset = 1
        if(lastReadBytes != -1) { //Can't simple add lastReadBytes to totalBytes without this check.
            //We read some bytes from the inputStream
            totalBytesInChunk += lastReadBytes;
        }

        lookAhead();
        return new ByteArrayInputStream(nextBytes, 0, totalBytesInChunk); //Length +1 for lookAheadByte
    }

    @Override
    public Iterator<ByteArrayInputStream> iterator() {
        return this;
    }

    @SneakyThrows
    private void lookAhead() {
        lookAheadByte = inputStream.read();
    }
}
