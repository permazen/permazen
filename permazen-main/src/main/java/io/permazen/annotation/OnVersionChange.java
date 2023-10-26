
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.annotation;

import io.permazen.UpgradeConversionPolicy;
import io.permazen.UntypedJObject;
import io.permazen.core.EnumValue;
import io.permazen.core.FieldType;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Annotation for methods that are to be invoked whenever an object's schema version has just changed,
 * in order to apply arbitrary "semantic" schema migration logic.
 *
 * <p>
 * The annotated method is given access to all of the previous object version's fields, including fields that have
 * been deleted or whose types have changed in the new schema version. This allows the object to perform any
 * schema migration "fixups" required before the old information is lost.
 *
 * <p>
 * Simple changes that only modify a simple field's type can often be handled automatically; see
 * {@link UpgradeConversionPolicy}, {@link JField#upgradeConversion &#64;JField.upgradeConversion()},
 * and {@link FieldType#convert FieldType.convert()} for details.
 *
 * <p><b>Method Parameters</b></p>
 *
 * <p>
 * The annotated method must be an instance method (i.e., not static), return void, and
 * take zero, one, two, or all three of the following parameters (in this order):
 * <ol>
 * <li>{@code int oldVersion} - previous schema version; required if {@link #oldVersion} is unspecified (i.e., zero)</li>
 * <li>{@code int newVersion} - new schema version; required if {@link #newVersion} is unspecified (i.e., zero)</li>
 * <li>{@code Map<Integer, Object> oldValues} <i>or</i> {@code Map<String, Object> oldValues} - immutable map containing
 *      all field values from the previous version of the object, indexed by either storage ID or field name.</li>
 * </ol>
 *
 * <p>
 * In addition to the above options, you may also completely ignore the schema version numbers by leaving {@link #oldVersion}
 * and {@link #newVersion} unspecified and declaring the method with only the {@code oldValues} parameter.
 * In many cases, this is the simplest way to handle schema changes: ignore version numbers and instead just using the
 * presence or absence of fields in {@code oldValues} to determine what migration work needs to be done. For example:
 * <pre>
 *      &#64;OnVersionChange
 *      private void applySchemaChanges(Map&lt;String, Object&gt; oldValues) {
 *          if (!oldValues.containsKey("balance"))      // at some point we added a new field "balance"
 *              this.setBalance(DEFAULT_BALANCE);
 *          if (oldValues.containsKey("fullName")) {    // we replaced "fullName" with "lastName" &amp; "firstName"
 *              final String fullName = (String)oldValues.get("fullName");
 *              if (fullName != null) {
 *                  final int comma = fullName.indexOf(',');
 *                  this.setLastName(comma == -1 ? null : fullName.substring(0, comma));
 *                  this.setFirstName(fullName.substring(comma + 1).trim());
 *              }
 *          }
 *          // ...etc
 *      }
 * </pre>
 *
 * <p>
 * If a class has multiple {@link OnVersionChange &#64;OnVersionChange}-annotated methods, methods with more specific
 * constraint(s) (i.e., non-zero {@link #oldVersion} and/or {@link #newVersion}) will be invoked first.
 *
 * <p><b>Incompatible Object Type Changes</b></p>
 *
 * <p>
 * Permazen supports arbitrary Java model schema changes across schema versions, including adding and removing Java types.
 * As a result, it's possible for non-primitive fields to have values that don't exist in the new schema. Therefore, it's
 * not possible to provide the old values to {@link OnVersionChange &#64;OnVersionChange} methods in their original form.
 *
 * <p>
 * Specifically, this can happen in two ways:
 * <ul>
 * <li>A reference field value refers to an object type that no longer exists; or</li>
 * <li>An {@link Enum} field refers to an {@link Enum} type that no longer exists, or whose constants have changed
 *      (this is really just a special case of the previous scenario: when an {@link Enum} type's constants change
 *      in any way, the new {@link Enum} is treated as a completely new type).</li>
 * </ul>
 *
 * <p>
 * Therefore, the following special rules apply to the values in the {@code oldValues} map:
 * <ul>
 * <li>For a reference field whose type no longer exists, the referenced object will be an {@link UntypedJObject}.
 * <li>For {@link Enum} fields, old values are always represented as {@link EnumValue} objects.
 *      For consistency's sake, this is true <i>even if the associated field's type has not changed</i>.</li>
 * </ul>
 *
 * <p><b>Meta-Annotations</b></p>
 *
 * <p>
 * This annotation may be configured indirectly as a Spring
 * <a href="https://docs.spring.io/spring/docs/current/spring-framework-reference/htmlsingle/#beans-meta-annotations">meta-annotation</a>
 * when {@code spring-core} is on the classpath.
 *
 * @see FieldType#convert FieldType.convert()
 * @see JField#upgradeConversion &#64;JField.upgradeConversion()
 */
@Retention(RetentionPolicy.RUNTIME)
@Target({ ElementType.ANNOTATION_TYPE, ElementType.METHOD })
@Documented
public @interface OnVersionChange {

    /**
     * Required old schema version.
     *
     * <p>
     * If this property is set to a positive value, only version changes
     * for which the previous schema version equals the specified version will result in notification,
     * and the annotated method must have the corresponding parameter omitted. Otherwise notifications
     * are delivered for any previous schema version and the {@code oldVersion} method parameter is required.
     *
     * <p>
     * Negative values are not allowed.
     *
     * @return old schema version, or zero for no restriction
     */
    int oldVersion() default 0;

    /**
     * Required new schema version.
     *
     * <p>
     * If this property is set to a positive value, only version changes
     * for which the new schema version equals the specified version will result in notification,
     * and the annotated method must have the corresponding parameter omitted. Otherwise notifications
     * are delivered for any new schema version and the {@code newVersion} method parameter is required.
     *
     * <p>
     * Negative values are not allowed.
     *
     * @return new schema version, or zero for no restriction
     */
    int newVersion() default 0;
}

