import static com.amazonaws.services.s3.internal.Constants.MB;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.InitiateMultipartUploadResult;
import com.amazonaws.services.s3.model.UploadPartResult;
import com.codahale.metrics.MetricRegistry;
import com.flipkart.fdp.superbi.d42.ChunkedInputStreamIterator;
import com.flipkart.fdp.superbi.d42.D42Client;
import com.flipkart.fdp.superbi.d42.D42Uploader;
import com.flipkart.fdp.superbi.d42.IteratorToInputStreamAdapter;
import com.google.common.collect.Iterators;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.retry.Retry;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.powermock.modules.junit4.PowerMockRunner;

@RunWith(PowerMockRunner.class)
public class D42UploaderTest {

  public static final String FILE_NAME_WITH_EXTENSION = "sample_file.csv";
  public static final int EXPIRE_AFTER_SECONDS = 10000;

  public static final int MAX_SIZE_PER_CHUNK = 10 * MB;
  public static final int MAX_ROW_SIZE = 100000;
  public static final String ROW_DELIMETER = "\n";
  public static final String D42_LINK = "d42Link";
  public static final String UPLOAD_ID = "uploadId";
  @Mock
  D42Client d42Client;

  @Mock
  AmazonS3 s3Client;

  @Mock
  UploadPartResult uploadPartResult;

  @Mock Iterator<String> iterator;

  private CircuitBreaker circuitBreaker = CircuitBreaker.ofDefaults("d42");
  private Retry retry = Retry.ofDefaults("d42");

  @Test
  public void testUploadWithSuccessCase(){
    D42Uploader d42Uploader = new D42Uploader(d42Client,circuitBreaker,retry, new MetricRegistry());

    Mockito.when(d42Client.getD42Connection()).thenReturn(s3Client);
    Mockito.when(d42Client.getBucket()).thenReturn("bucket");
    Mockito.when(d42Client.createD42Link(Mockito.any(),Mockito.anyString(),Mockito.anyLong())).thenReturn(
        D42_LINK);
    Mockito.when(s3Client.uploadPart(Mockito.any())).thenReturn(uploadPartResult);
    Mockito.when(s3Client.completeMultipartUpload(Mockito.any())).thenReturn(null);

    InitiateMultipartUploadResult initiateMultipartUploadResult = new InitiateMultipartUploadResult();
    initiateMultipartUploadResult.setUploadId(UPLOAD_ID);

    UploadPartResult uploadPartResult = new UploadPartResult();
    uploadPartResult.setPartNumber(2);
    uploadPartResult.setETag("eTag");

    Mockito.when(s3Client.uploadPart(Mockito.any())).thenReturn(uploadPartResult);
    Mockito.when(s3Client.initiateMultipartUpload(Mockito.any())).thenReturn(initiateMultipartUploadResult);

    List<String> strings = Arrays.asList(
        "A,5,Testing dfdf,10",
        "B,6,Testing dsff,11",
        "C,7,Testing 3eee,12",
        "D,8,Testing 4543535,1055",
        "E,9,Testing dsfybyvercwrrr,1"
    );
    Iterator<String> csvIterator = strings.iterator();
    Iterator<byte[]> csvByteIterator = Iterators.transform(csvIterator, s -> s.getBytes());

    IteratorToInputStreamAdapter iteratorInputStream = new IteratorToInputStreamAdapter(csvByteIterator,
        MAX_ROW_SIZE, ROW_DELIMETER);

    ChunkedInputStreamIterator inputStreams = new ChunkedInputStreamIterator(iteratorInputStream,
        MAX_SIZE_PER_CHUNK);

    String d42Link = d42Uploader.uploadInputStreamsToD42(EXPIRE_AFTER_SECONDS, FILE_NAME_WITH_EXTENSION,inputStreams);
    Assert.assertEquals(d42Link,D42_LINK);

  }

  @Test
  public void testUploadWithNoData(){
    D42Uploader d42Uploader = new D42Uploader(d42Client,circuitBreaker,retry, new MetricRegistry());

    Mockito.when(d42Client.getD42Connection()).thenReturn(s3Client);
    Mockito.when(d42Client.getBucket()).thenReturn("bucket");
    Mockito.when(d42Client.createD42Link(Mockito.any(),Mockito.anyString(),Mockito.anyLong())).thenReturn(
        D42_LINK);
    Mockito.when(s3Client.uploadPart(Mockito.any())).thenReturn(uploadPartResult);
    Mockito.when(s3Client.completeMultipartUpload(Mockito.any())).thenReturn(null);

    InitiateMultipartUploadResult initiateMultipartUploadResult = new InitiateMultipartUploadResult();
    initiateMultipartUploadResult.setUploadId(UPLOAD_ID);

    UploadPartResult uploadPartResult = new UploadPartResult();
    uploadPartResult.setPartNumber(2);
    uploadPartResult.setETag("eTag");

    Mockito.when(s3Client.uploadPart(Mockito.any())).thenReturn(uploadPartResult);
    Mockito.when(s3Client.initiateMultipartUpload(Mockito.any())).thenReturn(initiateMultipartUploadResult);

    List<String> strings = new ArrayList<>();
    Iterator<String> csvIterator = strings.iterator();
    Iterator<byte[]> csvByteIterator = Iterators.transform(csvIterator, s -> s.getBytes());

    IteratorToInputStreamAdapter iteratorInputStream = new IteratorToInputStreamAdapter(csvByteIterator,
        MAX_ROW_SIZE, ROW_DELIMETER);

    ChunkedInputStreamIterator inputStreams = new ChunkedInputStreamIterator(iteratorInputStream,
        MAX_SIZE_PER_CHUNK);

    d42Uploader.uploadInputStreamsToD42(EXPIRE_AFTER_SECONDS, FILE_NAME_WITH_EXTENSION,inputStreams);
    Mockito.verify(s3Client,Mockito.times(0)).uploadPart(Mockito.any());

  }

  @Test(expected = RuntimeException.class)
  public void testUploadWithException(){
    D42Uploader d42Uploader = new D42Uploader(d42Client,circuitBreaker,retry, new MetricRegistry());

    Mockito.when(d42Client.getD42Connection()).thenReturn(s3Client);
    Mockito.when(d42Client.getBucket()).thenReturn("bucket");
    Mockito.when(d42Client.createD42Link(Mockito.any(),Mockito.anyString(),Mockito.anyLong())).thenReturn(
        D42_LINK);
    Mockito.when(s3Client.uploadPart(Mockito.any())).thenReturn(uploadPartResult);
    Mockito.when(s3Client.completeMultipartUpload(Mockito.any())).thenReturn(null);
    Mockito.doNothing().when(s3Client).abortMultipartUpload(Mockito.any());

    InitiateMultipartUploadResult initiateMultipartUploadResult = new InitiateMultipartUploadResult();
    initiateMultipartUploadResult.setUploadId(UPLOAD_ID);

    UploadPartResult uploadPartResult = new UploadPartResult();
    uploadPartResult.setPartNumber(2);
    uploadPartResult.setETag("eTag");

    Mockito.when(s3Client.uploadPart(Mockito.any())).thenReturn(uploadPartResult);
    Mockito.when(s3Client.initiateMultipartUpload(Mockito.any())).thenReturn(initiateMultipartUploadResult);

    Mockito.when(iterator.hasNext()).thenReturn(true);
    Mockito.when(iterator.next()).thenThrow(new RuntimeException());

    Iterator<byte[]> csvByteIterator = Iterators.transform(iterator, s -> s.getBytes());

    IteratorToInputStreamAdapter iteratorInputStream = new IteratorToInputStreamAdapter(csvByteIterator,
        MAX_ROW_SIZE, ROW_DELIMETER);

    ChunkedInputStreamIterator inputStreams = new ChunkedInputStreamIterator(iteratorInputStream,
        MAX_SIZE_PER_CHUNK);

    d42Uploader.uploadInputStreamsToD42(EXPIRE_AFTER_SECONDS, FILE_NAME_WITH_EXTENSION,inputStreams);
    Mockito.verify(s3Client,Mockito.times(1)).abortMultipartUpload(Mockito.any());

  }
}
