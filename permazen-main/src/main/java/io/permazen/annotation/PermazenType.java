
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.annotation;

import io.permazen.Counter;
import io.permazen.JObject;
import io.permazen.Permazen;
import io.permazen.PermazenFactory;
import io.permazen.ReferencePath;
import io.permazen.StorageIdGenerator;
import io.permazen.UpgradeConversionPolicy;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Java annotation for Java classes that are {@link permazen.Permazen} object model types.
 *
 * <p>
 * Model classes may be POJO's, {@code abstract} classes, or interfaces, and the Java bean getter methods therein define database
 * fields; Permazen generates concrete subclasses of each model class at runtime.
 * Model classes must have a zero-arg constructor with at least {@code protected} access.
 *
 * <p>
 * The following annotations on the getter methods of a {@link PermazenType &#64;PermazenType}-annotated class
 * configure database fields:
 * <ul>
 *  <li>{@link JField &#64;JField} - defines a simple value, reference, or {@link Counter} field
 *  <li>{@link JSetField &#64;JSetField} - defines a {@link java.util.NavigableSet} field
 *  <li>{@link JListField &#64;JListField} - defines a {@link java.util.List} field
 *  <li>{@link JMapField &#64;JMapField} - defines a {@link java.util.NavigableMap} field
 * </ul>
 *
 * <p>
 * {@linkplain #autogenFields By default}, database fields are created automatically for all abstract getter methods,
 * so no annotations are required. To override the defaults, or define database fields for non-abstract getter methods,
 * add the appropriate {@link JField &#64;JField}, {@link JSetField &#64;JSetField}, {@link JListField &#64;JListField},
 * or {@link JMapField &#64;JMapField} annotation.
 *
 * <p>
 * These annotations are also supported on the methods in a {@link PermazenType &#64;PermazenType}-annotated class:
 *
 * <ul>
 *  <li>{@link OnChange &#64;OnChange} - annotates a method to be invoked when some field (possibly seen through
 *      a path of references) changes
 *  <li>{@link OnCreate &#64;OnCreate} - annotates a method to be invoked just after object creation
 *  <li>{@link OnDelete &#64;OnDelete} - annotates a method to be invoked just prior to object deletion
 *  <li>{@link OnValidate &#64;OnValidate} - annotates a method to be invoked whenever the object is (re)validated
 *  <li>{@link OnVersionChange &#64;OnVersionChange} - annotates a method to be invoked when the object's schema version changes
 *  <li>{@link FollowPath &#64;FollowPath} - annotates a method returning objects found by traversing a {@link ReferencePath}
 * </ul>
 *
 * <p>
 * Annotations are "inherited", meaning they may be present on any overridden supertype method, including methods in superclasses
 * and superinterfaces (whether or not annotated with {@link PermazenType &#64;PermazenType}).
 *
 * <p><b>{@link JObject} interface</b></p>
 *
 * <p>
 * The subclass of any {@link PermazenType &#64;PermazenType}-annotated model class that is generated by {@link Permazen}
 * will implement {@link JObject}, so the (abstract) model classes may be declared that way.
 *
 * <p><b>Indexing</b></p>
 *
 * <p>
 * Indexes on simple fields and collection fields are declared via {@link JField#indexed}.
 *
 * <p>
 * Composite indexes are declared by annotating any supertype of a {@link PermazenType &#64;PermazenType}-annotated class
 * with one or more {@link JCompositeIndex &#64;JCompositeIndex} annotations.
 *
 * <p><b>Meta-Annotations</b></p>
 *
 * <p>
 * This annotation may be configured indirectly as a Spring
 * <a href="https://docs.spring.io/spring/docs/current/spring-framework-reference/htmlsingle/#beans-meta-annotations">meta-annotation</a>
 * when {@code spring-core} is on the classpath.
 */
@Retention(RetentionPolicy.RUNTIME)
@Target({ ElementType.ANNOTATION_TYPE, ElementType.TYPE })
@Documented
public @interface PermazenType {

    /**
     * The name of this object type.
     *
     * <p>
     * If equal to the empty string (default value),
     * the {@linkplain Class#getSimpleName simple name} of the annotated Java class is used.
     *
     * @return object type name
     */
    String name() default "";

    /**
     * Storage ID for this object type. Value should be positive;
     * if zero, the configured {@link StorageIdGenerator} will be consulted to auto-generate a value.
     *
     * @return object type storage ID
     * @see StorageIdGenerator#generateClassStorageId StorageIdGenerator.generateClassStorageId()
     */
    int storageId() default 0;

    /**
     * Whether to automatically generate database fields from un-annotated abstract Java bean methods.
     *
     * <p>
     * If true, database fields will be auto-generated corresponding to all <b>abstract</b> bean property getter methods,
     * even if there is no {@link JField &#64;JField}, {@link JSetField &#64;JSetField},
     * {@link JListField &#64;JListField}, or {@link JMapField &#64;JMapField} annotation.
     * Note <i>this includes superclass and interface methods</i>. Methods must be either {@code public} or
     * {@code protected}. In the case of simple fields, there must also be a corresponding setter method.
     *
     * <p>
     * Getter methods with return type assignable to {@link java.util.Set}, {@link java.util.List}, and {@link java.util.Map}
     * will cause the corresponding collection fields to be created; other getter/setter method pairs will cause
     * the corresponding simple fields to be generated. Auto-generation of storage ID's is performed by the
     * configured {@link StorageIdGenerator}.
     *
     * <p>
     * A {@link JTransient &#64;JTransient} annotation on the getter method, or any overridden superclass method,
     * disables this auto-generation for that particular method (this is only useful when applied to non-abstract
     * methods, and therefore when {@link #autogenNonAbstract} is true: if applied to any abstract methods,
     * the auto-generated database subclass wouldn't be instantiable at runtime).
     *
     * @return whether to auto-generate fields from abstract methods
     * @see #autogenNonAbstract
     * @see PermazenFactory#setStorageIdGenerator PermazenFactory.setStorageIdGenerator()
     */
    boolean autogenFields() default true;

    /**
     * Whether to automatically generate database fields even from non-abstract Java bean methods.
     *
     * <p>
     * If {@link #autogenFields} is false, this property is ignored. Otherwise, database fields will be auto-generated
     * corresponding to all bean property getter methods, whether or not marked <b>abstract</b>, unless there is
     * a {@link JTransient &#64;JTransient} annotation.
     *
     * @return whether to auto-generate fields from non-abstract methods when {@link #autogenFields} is true
     * @see #autogenFields
     * @see JTransient
     */
    boolean autogenNonAbstract() default false;

    /**
     * Configure the default for the {@link JField#allowDeleted &#64;JField.allowDeleted()} property
     * for auto-generated reference fields.
     *
     * <p>
     * If {@link #autogenFields} is false, this property is ignored. Otherwise, if this property is true,
     * any auto-generated reference fields will allow assignment to deleted objects in normal transactions.
     * In other words, they will behave as if they had a {@link JField &#64;JField} annotation with {@link JField#allowDeleted}
     * set to true.
     *
     * @return whether auto-generated reference fields should allow assignment to a deleted object in normal transactions
     * @see JField#allowDeleted
     */
    boolean autogenAllowDeleted() default false;

    /**
     * Configure the default for the {@link JField#allowDeletedSnapshot &#64;JField.allowDeletedSnapshot()} property
     * for auto-generated reference fields.
     *
     * <p>
     * If {@link #autogenFields} is false, this property is ignored. Otherwise, if this property is true,
     * any auto-generated reference fields will allow assignment to deleted objects in snapshot transactions.
     * In other words, they will behave as if they had a {@link JField &#64;JField} annotation with
     * {@link JField#allowDeletedSnapshot} set to true.
     *
     * @return whether auto-generated reference fields should allow assignment to a deleted object in snapshot transactions
     * @see JField#allowDeletedSnapshot
     */
    boolean autogenAllowDeletedSnapshot() default true;

    /**
     * Configure the default for the {@link JField#upgradeConversion &#64;JField.upgradeConversion()} property
     * for auto-generated reference fields.
     *
     * <p>
     * If {@link #autogenFields} is false, this property is ignored. Otherwise, any auto-generated fields will
     * have the specified {@link UpgradeConversionPolicy} applied when upgrading an object from some other schema
     * version to the current schema version.
     *
     * @return type conversion policy for auto-generated fields
     * @see JField#upgradeConversion
     */
    UpgradeConversionPolicy autogenUpgradeConversion() default UpgradeConversionPolicy.ATTEMPT;
}

