package com.flipkart.fdp.superbi.cosmos.meta.api;

import com.flipkart.fdp.superbi.cosmos.meta.model.external.WebSchema;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;


/**
 * Created by jalaj.kumar on 04/03/15.
 */
@Getter
@NoArgsConstructor(access = AccessLevel.PUBLIC)
@Setter
@AllArgsConstructor(access = AccessLevel.PUBLIC)
public class SchemaChange {

    private String changeType;

    private WebSchema oldSchema;

    private WebSchema newSchema;

}
