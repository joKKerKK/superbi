package com.flipkart.fdp.superbi.core.model;

import lombok.AllArgsConstructor;
import lombok.Getter;

@AllArgsConstructor
public enum OperationType {
    READ(0);

    @Getter
    private int value;
}
