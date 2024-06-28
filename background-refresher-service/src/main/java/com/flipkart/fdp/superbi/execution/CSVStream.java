package com.flipkart.fdp.superbi.execution;

import com.flipkart.fdp.superbi.execution.exception.DataSizeLimitExceedException;
import com.flipkart.fdp.superbi.refresher.dao.result.QueryResult;
import com.flipkart.fdp.superbi.utils.JsonUtil;
import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import java.text.DecimalFormat;
import java.text.MessageFormat;
import java.text.NumberFormat;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Spliterator;
import java.util.stream.BaseStream;
import javax.validation.constraints.NotNull;
import lombok.Getter;
import org.apache.commons.lang3.StringEscapeUtils;
import org.apache.commons.lang3.StringUtils;
import org.checkerframework.checker.nullness.qual.Nullable;

public class CSVStream implements BaseStream<String, CSVStream> {
    private final QueryResult queryResult;
    private final List<String> headers;
    private final long dataSizeLimit;

    @Getter
    private int rowCount = 0;

    @Getter
    private long dataSize;

    public CSVStream(@NotNull QueryResult queryResult, long dataSizeLimit) {
        this.queryResult = queryResult;
        this.headers = queryResult.getColumns();
        this.dataSizeLimit = dataSizeLimit;
    }

    @Override
    public Iterator<String> iterator() {
        long dataSizeLimitInGBs = dataSizeLimit/(1024*1024*1024);
        Iterator<String> headerIterator = Arrays.asList(getHeaderRow()).iterator();
        Iterator<String> endOfFileIterator = Arrays.asList(getEndOfFIle()).iterator();
        Iterator<String> queryResultIterator =  Iterators.transform(queryResult.iterator(), new Function<List<Object>, String>() {
            @Nullable
            @Override
            public String apply(@Nullable List<Object> columnValues) {
                final StringBuilder builder = new StringBuilder();
                Joiner.on(",").useForNull(StringUtils.EMPTY).appendTo(builder, Iterables.transform(columnValues, columnValueTransform));
                return builder.toString();
            }
        });
        Iterator<String> resultIterator = Iterators.concat(headerIterator,queryResultIterator,endOfFileIterator);
        return new Iterator<String>() {
            @Override
            public boolean hasNext() {
                if(dataSize > dataSizeLimit){
                    throw new DataSizeLimitExceedException(MessageFormat.format("Data Size limit {0} GBs exceed",dataSizeLimitInGBs));
                }
                return resultIterator.hasNext();
            }

            @Override
            public String next() {
                String next = resultIterator.next();
                dataSize += JsonUtil.getJsonSize(next);
                rowCount++;
                return next;
            }
        };
    }

    @Override
    public Spliterator<String> spliterator() {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isParallel() {
        return false;
    }

    @Override
    public CSVStream sequential() {
        return this;
    }

    @Override
    public CSVStream parallel() {
        return this;
    }

    @Override
    public CSVStream unordered() {
        return this;
    }

    @Override
    public CSVStream onClose(Runnable closeHandler) {
        // Ignore
        return this;
    }

    @Override
    public void close() {
        queryResult.close();
    }

    public static Function<Object, String> columnValueTransform = s -> {
        //DecimalFormatter preserves decimal digits whereas bigdecimal expands and changes fraction value
        NumberFormat formatter = new DecimalFormat(".##########");
        if (s instanceof Double) {
            Double d = (Double) s;
            String flattened = (d.isInfinite() || d.isNaN()) ? String.valueOf(d) : formatter.format(d);
            return StringEscapeUtils.escapeCsv(flattened);
        } else if (s instanceof Float) {
            Float f = (Float) s;
            String flattened = (f.isInfinite() || f.isNaN()) ? String.valueOf(f) : formatter.format(f);
            return StringEscapeUtils.escapeCsv(flattened);
        }
        return StringEscapeUtils.escapeCsv(StringEscapeUtils.escapeJava(String.valueOf(s)));
    };

    public String getHeaderRow() {
        return Joiner.on(",").join(headers);
    }

    public String getEndOfFIle() {
        return "END_OF_FILE";
    }

    public String getRowSeperator() {
        return "\n";
    }
}
