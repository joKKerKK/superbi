package com.flipkart.fdp.superbi.refresher.dao.validation;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;

import java.util.List;
import java.util.Map;

@Builder
@Getter
@NoArgsConstructor
@AllArgsConstructor
@ToString
public class BatchCubeGuardrailConfig {
    private List<String> batchCubesFactList;
    private boolean globalEnablePartitionKeyValidation;
}
