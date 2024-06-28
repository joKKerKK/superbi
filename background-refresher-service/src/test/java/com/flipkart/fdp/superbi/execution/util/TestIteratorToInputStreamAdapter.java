package com.flipkart.fdp.superbi.execution.util;

public class TestIteratorToInputStreamAdapter {

//    //###
//    //### Utility Methods
//    //###
//
//    @SneakyThrows
//    private void assertSanity(String expected, IteratorToInputStreamAdapter iteratorInputStream, int nBytesToRead) {
//        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream(10);
//        byte[] bytes = new byte[nBytesToRead];
//        int read =  iteratorInputStream.read(bytes, 0, bytes.length);
//        while(read != -1) {
//            byteArrayOutputStream.write(bytes, 0, read);
//            read = iteratorInputStream.read(bytes, 0, bytes.length);
//        }
//
//        Assert.assertEquals(expected, new String(byteArrayOutputStream.toByteArray()));
//    }
//
//    @SneakyThrows
//    private void assertSanitySingleRead(String expected, IteratorToInputStreamAdapter iteratorInputStream) {
//        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream(10);
//        int read =  iteratorInputStream.read();
//        while(read != -1) {
//            byteArrayOutputStream.write(read);
//            read = iteratorInputStream.read();
//        }
//
//        Assert.assertEquals(expected, new String(byteArrayOutputStream.toByteArray()));
//    }
//
//    public void testMultipleRows0(int nBytesToRead){
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
//        if(nBytesToRead == 1) {
//            assertSanitySingleRead(expected, iteratorInputStream);
//        } else {
//            assertSanity(expected, iteratorInputStream, nBytesToRead);
//        }
//    }
//
//    //###
//    //### Test Cases
//    //###
//
//    @Test
//    public void testMultipleRows(){
//        testMultipleRows0(18);
//    }
//
//    @Test
//    public void testNoRows() {
//        Iterator<byte[]> iterator = Arrays.<byte[]>asList().iterator();
//        IteratorToInputStreamAdapter iteratorInputStream = new IteratorToInputStreamAdapter(iterator, 30, "\n");
//        assertSanity("", iteratorInputStream, 18);
//    }
//
//    @Test
//    public void testOnlyOneRow() {
//        String expected = "Only one row";
//        Iterator<byte[]> iterator = Arrays.asList(expected.getBytes()).iterator();
//        IteratorToInputStreamAdapter iteratorInputStream = new IteratorToInputStreamAdapter(iterator, 30, "\n");
//        assertSanity(expected, iteratorInputStream, 18);
//    }
//
//    @Test
//    public void testReadOnlyOneByte() {
//        testMultipleRows0(1);
//    }
//
//    @Test
//    public void testReadSomeRandomBytes() {
//        testMultipleRows0(23);
//    }
//
//    @Test
//    public void testReadLargeNumberOfBytes() {
//        testMultipleRows0(10000);
//    }
//
//    @Test
//    public void testSingleReadMethod() {
//        String expected = "Only one row";
//        Iterator<byte[]> iterator = Arrays.asList(expected.getBytes()).iterator();
//        IteratorToInputStreamAdapter iteratorInputStream = new IteratorToInputStreamAdapter(iterator, 30, "\n");
//        assertSanity(expected, iteratorInputStream, 18);
//
//    }
}