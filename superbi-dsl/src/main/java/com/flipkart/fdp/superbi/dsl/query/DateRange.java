package com.flipkart.fdp.superbi.dsl.query;

import com.google.common.base.Optional;
import java.util.Date;

/**
 * User: shashwat
 * Date: 02/01/14
 */
public class DateRange {
    private String column;
    private Optional<String> timeColumn = Optional.absent();
    private Date start, end = new Date();
    private Optional<Long> intervalMs = Optional.absent();

    public DateRange() {
    }

    private DateRange(DateRange that) {
        this.column = that.column;
        this.timeColumn = that.timeColumn;
        this.start = that.start;
        this.end = that.end;
        this.intervalMs = that.intervalMs;
    }

    public Date getStart() {
        return start;
    }

    public Date getEnd() {
        return end;
    }

    public Optional<Long> getIntervalMs() {
        return intervalMs;
    }

    public String getColumn(){
        return column;
    }

    public Optional<String> getTimeColumn() {
        return timeColumn;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static Builder from(DateRange that) {
        return new Builder(that);
    }

    public static class Builder {
        private final DateRange dateRange;

        private Builder(DateRange dateRange) {
            this.dateRange = new DateRange(dateRange);
        }

        private Builder() {
            dateRange = new DateRange();
        }

        public Builder column(String column){
            dateRange.column = column;
            return this;
        }

        public Builder start(Date date) {
            dateRange.start = date;
            // TODO: check if it is greater than endDate
            return this;
        }

        public Builder start(long ts) {
            return start(new Date(ts));
        }

        public Builder end(Date date) {
            dateRange.end = date;
            // TODO: check if it is less than startDate
            return this;
        }

        public Builder end(long ts) {
            return end(new Date(ts));
        }

        public Builder interval(long intervalMs) {
            return interval(Optional.of(intervalMs));
        }

        public Builder timeColumn(String timeColumn) {
            return timeColumn(Optional.of(timeColumn));
        }

        public Builder timeColumn(Optional<String> timeColumn) {
            dateRange.timeColumn = timeColumn;
            return this;
        }

        public Builder interval(Optional<Long> intervalMs) {
            dateRange.intervalMs = intervalMs; // TODO: check if it is greater than zero
            return this;
        }


        public DateRange build() {
            return new DateRange(dateRange);
        }
    }
}
