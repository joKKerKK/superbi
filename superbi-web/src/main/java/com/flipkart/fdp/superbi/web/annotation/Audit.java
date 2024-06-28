package com.flipkart.fdp.superbi.web.annotation;

import com.google.inject.BindingAnnotation;

import javax.ws.rs.NameBinding;
import java.lang.annotation.*;

@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.TYPE})
@Inherited
public @interface Audit {
    int orgAt();

    int namespaceAt();

    int nameAt();
}
