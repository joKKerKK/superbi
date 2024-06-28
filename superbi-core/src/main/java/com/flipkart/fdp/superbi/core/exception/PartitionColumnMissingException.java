package com.flipkart.fdp.superbi.core.exception;

import lombok.AllArgsConstructor;

import java.text.MessageFormat;

@AllArgsConstructor
public class PartitionColumnMissingException extends RuntimeException{
    private static final String MESSAGE_TEMPLATE = "Underlying query with fact {0} does not have a column which is partitioned";
    private final String fact;

    @Override
    public String getMessage() {
        return MessageFormat.format(MESSAGE_TEMPLATE, fact);
    }
}
