
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.annotation;

import io.permazen.JCounterField;
import io.permazen.UntypedJObject;
import io.permazen.UpgradeConversionPolicy;
import io.permazen.core.EnumValue;
import io.permazen.core.TypeNotInSchemaException;
import io.permazen.encoding.Encoding;
import io.permazen.schema.SchemaId;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Annotation for methods that are to be invoked whenever an object's schema has just changed,
 * in order to apply arbitrary "semantic" schema migration logic.
 *
 * <p>
 * The annotated method is given access to all of the previous object version's fields, including fields that have
 * been deleted or whose types have changed in the new schema. This allows the object to perform any schema migration
 * "fixups" that may be required before the old information is lost for good.
 *
 * <p>
 * Simple changes that only modify a simple field's type can often be handled automatically; see
 * {@link UpgradeConversionPolicy}, {@link JField#upgradeConversion &#64;JField.upgradeConversion()},
 * and {@link Encoding#convert Encoding.convert()} for details.
 *
 * <p>
 * The annotated method must be an instance method (i.e., not static), return void, and take from one to three parameters.
 * The first parameter must have type {@code Map<String, Object> oldValues} and will be an immutable map containing the values
 * of all the fields in the previous schema version of the object indexed by field name. The optional second and third
 * parameters have type {@link SchemaId} and identify the old and new schemas (respectively).
 *
 * <p>
 * In many cases, the simplest way to handle schema changes is to use the presence or absence of fields in {@code oldValues}
 * to determine what migration work needs to be done. For example:
 * <pre>
 *      &#64;OnSchemaChange
 *      private void applySchemaChanges(Map&lt;String, Object&gt; oldValues) {
 *
 *          // At some point we added a new field "balance"
 *          if (!oldValues.containsKey("balance"))
 *              this.setBalance(this.calculateBalanceForSchemaMigration());
 *
 *          // At some point we replaced "fullName" with "lastName" &amp; "firstName"
 *          if (oldValues.containsKey("fullName")) {
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
 * Note: {@linkplain JCounterField} values are represented in {@code oldValues} as {@code Long}s.
 *
 * <p>
 * A class may have multiple {@link OnSchemaChange &#64;OnSchemaChange}-annotated methods.
 *
 * <p><b>Incompatible Object Type Changes</b></p>
 *
 * <p>
 * Permazen supports arbitrary Java model schema changes across schemas, including adding and removing Java types.
 * This creates a few caveats relating to schema migration.
 *
 * <p>
 * First, if an object's type no longer exists in the new schema, migration is not possible, and any attempt to do so will
 * throw a {@link TypeNotInSchemaException}. Such objects are still accessible however (see {@link UntypedJObject}).
 *
 * <p>
 * Secondly, it's possible for an old field to have a value that simply doesn't exist in the new schema. When this happens,
 * it's not possible to provide the old value to an {@link OnSchemaChange &#64;OnSchemaChange} method in its original form.
 * This can happen in two ways:
 * <ul>
 * <li>A reference field refers to an object whose type no longer exists in the new schema; or</li>
 * <li>An {@link Enum} field refers to an {@link Enum} type that no longer exists, or whose identifiers have changed
 *      (this is really just a special case of the previous scenario: when an {@link Enum} type's constants change
 *      in any way, the new {@link Enum} is treated as a completely new type).</li>
 * </ul>
 *
 * <p>
 * Therefore, the following special rules apply to the {@code oldValues} map:
 * <ul>
 * <li>For a reference field whose type no longer exists, the referenced object will appear as an {@link UntypedJObject}.
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
 * @see Encoding#convert Encoding.convert()
 * @see JField#upgradeConversion &#64;JField.upgradeConversion()
 */
@Retention(RetentionPolicy.RUNTIME)
@Target({ ElementType.ANNOTATION_TYPE, ElementType.METHOD })
@Documented
public @interface OnSchemaChange {
}
