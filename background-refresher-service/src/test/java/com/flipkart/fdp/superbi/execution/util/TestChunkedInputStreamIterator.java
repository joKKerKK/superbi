package com.flipkart.fdp.superbi.execution.util;

public class TestChunkedInputStreamIterator {
//
//    //###
//    //### Utility Methods
//    //###
//
//    @SneakyThrows
//    private void assertSanity(int MAX_SIZE_PER_CHUNK, String expected, ChunkedInputStreamIterator inputStreams) {
//        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream(10);
//
//        while(inputStreams.hasNext()) {
//            ByteArrayInputStream is = inputStreams.next();
//            //If it is not the last one
//            if(inputStreams.hasNext()) {
//                assert is.available() == MAX_SIZE_PER_CHUNK;
//            }
//            else{
//                assert is.available() <= MAX_SIZE_PER_CHUNK;
//            }
//            IOUtils.copy(is, byteArrayOutputStream);
//        }
//
//        Assert.assertEquals(expected, new String(byteArrayOutputStream.toByteArray()));
//    }
//
//    public void testLargeInputStream_multipleChunks(int MAX_SIZE_PER_CHUNK) {
//        List<String> strings = Arrays.asList(
//                "A,5,Testing dfdf,10",
//                "B,6,Testing dsff,11",
//                "C,7,Testing 3eee,12",
//                "D,8,Testing 4543535,1055",
//                "E,9,Testing dsfybyvercwrrr,1"
//        );
//        String expected = Strings.join(strings, "\n");
//        ByteArrayInputStream inputStream = new ByteArrayInputStream(expected.getBytes());
//        ChunkedInputStreamIterator inputStreams = new ChunkedInputStreamIterator(inputStream, MAX_SIZE_PER_CHUNK);
//
//        assertSanity(MAX_SIZE_PER_CHUNK, expected, inputStreams);
//    }
//
//    //###
//    //### Test cases
//    //###
//
//    @Test
//    public void testEmptyInputStream_zeroChunks() {
//        int MAX_SIZE_PER_CHUNK = 10;
//        ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(new byte[0]);
//        ChunkedInputStreamIterator inputStreams = new ChunkedInputStreamIterator(byteArrayInputStream, MAX_SIZE_PER_CHUNK);
//        assert !inputStreams.hasNext();
//    }
//
//    @Test
//    public void testSmallInputStream_oneChunk() {
//        int MAX_SIZE_PER_CHUNK = 100;
//        String expected = "ABC";
//        ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(expected.getBytes());
//        ChunkedInputStreamIterator inputStreams = new ChunkedInputStreamIterator(byteArrayInputStream, MAX_SIZE_PER_CHUNK);
//
//        assertSanity(MAX_SIZE_PER_CHUNK, expected, inputStreams);
//    }
//
//    @Test(expected = IllegalArgumentException.class)
//    public void testMultipleChunks_chunkSizeZero() {
//        testLargeInputStream_multipleChunks(0);
//    }
//
//    @Test
//    public void testMultipleChunks_chunkSizeOne() {
//        testLargeInputStream_multipleChunks(1);
//    }
//
//    @Test
//    public void testMultipleChunks_chunkSizeRandom() {
//        testLargeInputStream_multipleChunks(13);
//    }
//
//    @Test
//    public void testMultipleChunks_chunkSizeLarge() {
//        testLargeInputStream_multipleChunks(10000); //This is same as testSmallInputStream_oneChunk
//    }
//
//    @Test
//    public void testIntegrationWithIteratorToInputStreamAdapter() {
//        int MAX_SIZE_PER_CHUNK = 24;
//        List<String> strings = Arrays.asList(
//                "A,5,Testing dfdf,10",
//                "B,6,Testing dsff,11",
//                "C,7,Testing 3eee,12",
//                "D,8,Testing 4543535,1055",
//                "E,9,Testing dsfybyvercwrrr,1"
//        );
//        String ROW_DELIMETER = "\n";
//        String expected = Strings.join(strings, ROW_DELIMETER);
//        Iterator<byte[]> iterator = Iterators.transform(strings.iterator(), s -> s.getBytes());
//
//        IteratorToInputStreamAdapter iteratorInputStream = new IteratorToInputStreamAdapter(iterator, 30, ROW_DELIMETER);
//
//        ChunkedInputStreamIterator inputStreams = new ChunkedInputStreamIterator(iteratorInputStream, MAX_SIZE_PER_CHUNK);
//
//        assertSanity(MAX_SIZE_PER_CHUNK, expected, inputStreams);
//
//    }
//
//    @SneakyThrows
//    @Test
//    public void testIntegrationWithIteratorToInputStreamAdapter_exactlyTwoChunks() {
//        int MAX_SIZE_PER_CHUNK = 25;
//        List<String> strings = Arrays.asList(
//                "A,5,Testing dfdf,10",
//                "B,6,Testing dsff,11",
//                "C,7,Testin"
//        ); //this is exactly 50 bytes with ROW_DELIMETER ADDED
//        String ROW_DELIMETER = "\n";
//        String expected = Strings.join(strings, ROW_DELIMETER);
//
//        Iterator<byte[]> iterator = Iterators.transform(strings.iterator(), s -> s.getBytes());
//
//        IteratorToInputStreamAdapter iteratorInputStream = new IteratorToInputStreamAdapter(iterator, 30, ROW_DELIMETER);
//
//        ChunkedInputStreamIterator inputStreams = new ChunkedInputStreamIterator(iteratorInputStream, MAX_SIZE_PER_CHUNK);
//
//        assertSanity(MAX_SIZE_PER_CHUNK, expected, inputStreams);
//
//    }
}