package com.flipkart.fdp.superbi.subscription.delivery;

import com.flipkart.fdp.superbi.subscription.model.CSVStream;
import com.flipkart.fdp.superbi.subscription.model.RawQueryResultWithSchema;
import java.io.ByteArrayOutputStream;
import java.text.MessageFormat;
import java.util.List;
import java.util.stream.Collectors;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class CsvUtil {

  @SneakyThrows
  public static ByteArrayOutputStream buildCsvByteStreamFromData(RawQueryResultWithSchema queryResult,String reportName){
    List<String> headers = queryResult.getSchema().columns.stream().filter(i->i.isVisible() == true).map(i->i.getAlias()).collect(
        Collectors.toList());
    try(CSVStream csvStream = new CSVStream(queryResult.getData().iterator(),headers)){
      try(ByteArrayOutputStream out = new ByteArrayOutputStream()){
        csvStream.iterator().forEachRemaining(stream-> {
          try {
            out.write(stream.getBytes());
            out.write(csvStream.getRowSeperator().getBytes());
          } catch (Exception e) {
            log.error(MessageFormat.format("Error creating csv for report {0}",reportName));
          }
        });
        return out;
      }
    }catch (Exception e){
      log.error(MessageFormat.format("Error creating csv for report {0}",reportName));
      throw e;
    }
  }
}
