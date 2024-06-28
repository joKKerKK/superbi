package com.flipkart.fdp.superbi.subscription.delivery;

import static com.amazonaws.services.s3.internal.Constants.MB;

import com.flipkart.fdp.superbi.d42.ChunkedInputStreamIterator;
import com.flipkart.fdp.superbi.d42.D42Uploader;
import com.flipkart.fdp.superbi.d42.IteratorToInputStreamAdapter;
import com.flipkart.fdp.superbi.gcs.GcsUploader;
import com.flipkart.fdp.superbi.subscription.model.CSVStream;
import com.flipkart.fdp.superbi.subscription.model.RawQueryResultWithSchema;
import com.google.common.collect.Iterators;
import com.google.inject.Inject;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

@Slf4j
public class D42Util {

  public static final String FILE_FORMAT = ".csv";
  private final D42Uploader d42Uploader;
  private final GcsUploader gcsUploader;
  public static final int MAX_SIZE_PER_CHUNK = 10 * MB;
  public static final int MAX_ROW_SIZE = 100000;

  @Inject
  public D42Util(D42Uploader d42Uploader, GcsUploader gcsUploader) {
    this.d42Uploader = d42Uploader;
    this.gcsUploader = gcsUploader;
  }

  @SneakyThrows
  public String uploadDataInD42(RawQueryResultWithSchema queryResult, String reportName, long d42ExpiryInSeconds, String cacheKey) {
    String fileName = reportName + '_' + new Date().getTime();
    List<String> headers = queryResult.getSchema().columns.stream().filter(i->i.isVisible() == true).map(i->i.getAlias()).collect(
        Collectors.toList());
    try(CSVStream csvStream = new CSVStream(queryResult.getData().iterator(),headers)){

      final String fileNameWithExtension = fileName + FILE_FORMAT;

      Iterator<String> csvIterator = csvStream.iterator();
      Iterator<byte[]> csvByteIterator = Iterators.transform(csvIterator, s -> s.getBytes());

      IteratorToInputStreamAdapter iteratorInputStream = new IteratorToInputStreamAdapter(csvByteIterator,
          MAX_ROW_SIZE, csvStream.getRowSeperator());
      ChunkedInputStreamIterator inputStreams = new ChunkedInputStreamIterator(iteratorInputStream,
          MAX_SIZE_PER_CHUNK);
      String d42Link = StringUtils.EMPTY;
      if(gcsUploader.allowedDatastores.stream().anyMatch(cacheKey::contains)){
        log.info("Uploading to GCS " + fileNameWithExtension);
        d42Link = gcsUploader.uploadInputStreamsToGcs(d42ExpiryInSeconds,fileNameWithExtension,inputStreams);
        log.info("Uploading to GCS " + fileNameWithExtension + " complete");
      } else{
        d42Link = d42Uploader.uploadInputStreamsToD42(d42ExpiryInSeconds,fileNameWithExtension,inputStreams);
      }
      return d42Link;
    }
  }
}
