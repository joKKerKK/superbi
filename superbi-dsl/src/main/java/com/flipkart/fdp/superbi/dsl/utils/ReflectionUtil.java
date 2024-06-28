package com.flipkart.fdp.superbi.dsl.utils;

import com.google.common.collect.Lists;
import java.lang.annotation.Annotation;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.proxy.ProxyFactory;
import org.apache.commons.proxy.invoker.DuckTypingInvoker;
import org.apache.commons.proxy.provider.ConstantProvider;
import org.reflections.ReflectionUtils;
import org.reflections.scanners.AbstractScanner;
import org.reflections.util.ClasspathHelper;
import org.reflections.util.ConfigurationBuilder;

/**
 * Created by IntelliJ IDEA.
 * User: vishnuhr
 * Date: 13/01/13
 * Time: 9:22 AM
 */
public final class ReflectionUtil {
    /**
     * get Methods With a specified Annotation in a class.
     *
     * @param annotation_name - the name of annotation to search for on methods
     * @param klass           - the class to search in
     * @return List<Method> having the provided annotation_name . This list can be empty.
     */
    public static Map<Method, Annotation> getMethodsWithAnnotation(String annotation_name, Class klass) {
        Method[] methods = klass.getMethods();
        Map<Method, Annotation> method_annotation_map = new HashMap<Method, Annotation>();
        for (Method method : methods) {
            Annotation[] annotations = method.getAnnotations();
            for (Annotation annotation : annotations) {
                String given_annotation_name = annotation.annotationType().getSimpleName();
                if (annotation_name.equalsIgnoreCase(given_annotation_name)) {
                    method_annotation_map.put(method, annotation);
                }
            }
        }
        return method_annotation_map;
    }

    /**
     * get Setter Method based on getter method name.
     * Assumes a strict contract in naming such methods. example: if getter is called getWidth then setter would be setWidth.
     * @param getter_method_name - example: getWidth
     * @return the setter method OR null,when provided getter_method_name is null or does not start with 'get'
     */
    /**
     * get Setter Method based on getter method name.
     * Assumes a strict contract in naming such methods. example: if getter is called getWidth then setter would be setWidth.
     *
     * @param getter_method_name - example: getWidth
     * @param klass              - the class object
     * @return the setter method
     * @throws NoSuchMethodException if getter has no corresponding setter or if getter_method_name is invalid(i.e. null or does not start with 'get')
     */
    public static Method getSetterMethod(String getter_method_name, Class klass) throws NoSuchMethodException {
        if (getter_method_name == null || !getter_method_name.startsWith("get")) {
            throw new NoSuchMethodException("Cannot get Setter method. Getter Method name:" + getter_method_name + ", provided is not valid");
        }
        String setter_method_name = getter_method_name.replaceFirst("get", "set");
        Method[] methods = klass.getMethods();
        Method setter = null;
        for (Method method : methods) {
            if (method.getName().equalsIgnoreCase(setter_method_name)) {   //todo: using equalsIgnoreCase ?? mite need rethink
                setter = method;
                break;
            }
        }
        if (setter == null) {
            throw new NoSuchMethodException("Cannot get Setter method. The Getter Method:" + getter_method_name + ", has no Corresponding Setter method. Do Check access modifier for Setter method.");
        }
        return setter;
    }


    /**
     * Cast the object into another class if the object implements all the methods as the class.
     * See duck typing
     * @param o Object that needs to be cast
     * @param klass Interface/class that this object needs to be cast into
     * @param <T> klass Type
     * @return a proxy instance of the object. If the object does not implement all methods,
     * you will get RuntimeExceptions.
     */
    @SuppressWarnings("unchecked")
    public static <T> T duckTypeCast(Object o, Class<T> klass) {
        if (klass.isInstance(o)) {
            return klass.cast(o);
        }
        return (T) new ProxyFactory().createInvokerProxy(
            new DuckTypingInvoker(new ConstantProvider(o)),
            new Class[]{klass});
    }


    public static class PackageScanner {
        private final String packageName;

        public PackageScanner(String packageName) {
            this.packageName = packageName;
        }

        public void scan(final ScanCallback callback) {
            final AbstractScanner packageScanner = new AbstractScanner() {
                @Override
                public void scan(Object cls) {
                    final String className = getMetadataAdapter().getClassName(cls);
                    final Class klass = (cls instanceof Class)? (Class<?>)cls: ReflectionUtils.forName(className);

                    if (className.startsWith(packageName)) {
                        callback.apply(klass);
                    }
                }
            };

            new ConfigurationBuilder().
                addUrls(ClasspathHelper.forPackage(packageName)).
                setScanners(packageScanner).
                build(); // will call scan and hence the scanner

        }
    }


    public interface ScanCallback {
        void apply(Class klass);
    }
    public static<T> List<T> getAs(List<Object> allEntities, Class<T> schedulableClass) {
        final List<T> list = Lists.newArrayList();
        for( Object o : allEntities) {
            list.add((T) o);
        }
        return list;
    }

    private ReflectionUtil() {
    }
}
