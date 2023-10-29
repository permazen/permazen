
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen;

import io.permazen.annotation.JField;
import io.permazen.annotation.OnVersionChange;
import io.permazen.core.DeleteAction;
import io.permazen.encoding.Encoding;

/**
 * Policies to apply when a simple or counter field's type changes during a schema update.
 *
 * <p>
 * <b>Type Changes</b>
 *
 * <p>
 * Permazen fields are identified by their {@linkplain JSimpleField#getStorageId storage ID's}, which is typically
 * {@linkplain StorageIdGenerator derived automatically} from the field's name.
 * With one restriction<sup>*</sup>, the type of a field may change arbitrarily between schema versions.
 *
 * <p>
 * When changing an object's schema version, Permazen supports optional automatic conversion of simple field
 * values from the old type to the new type. For example, an {@code int} field value {@code 1234} can be automatically
 * converted into {@link String} field value {@code "1234"}.
 *
 * <p>
 * See {@link Encoding#convert} for details about conversions between simple encodings. In addition,
 * {@link Counter} fields can be converted to/from any numeric Java primitive (or primitive wrapper) type.
 *
 * <p>
 * This class is used to {@linkplain JField#upgradeConversion specify} whether such automatic
 * conversion should occur when a simple field's type changes, and if so, whether the conversion must always succeed.
 *
 * <p>
 * <sup>*</sup>A simple field may not have different types across schema versions and be indexed in both versions.
 *
 * <p>
 * <b>References and Enums</b>
 *
 * <p>
 * Permazen considers {@link Enum} types with different identifier lists as different types. However, automatic
 * conversion of {@link Enum} values in simple fields will work if the existing value's name is valid for the new
 * {@link Enum} type.
 *
 * <p>
 * Automatic conversion of reference fields works as long as the referenced object's type is assignable to the field's
 * new Java type; otherwise, the field is {@linkplain DeleteAction#UNREFERENCE unreferenced},
 * i.e., set to null (if a simple field) or removed (if an element in a complex field).
 *
 * <p>
 * <b>Conversion Policies</b>
 *
 * <p>
 * With {@link #RESET}, no automatic conversion is attempted: the field is always reset to the default value of
 * the new type. With {@link #ATTEMPT} and {@link #REQUIRE}, automatic conversion of field values is attempted.
 *
 * <p>
 * For some types and/or field values, conversion is not possible. In this case, {@link #REQUIRE} generates a
 * {@link UpgradeConversionException}, while {@link #ATTEMPT} just reverts to the behavior of {@link #RESET}.
 *
 * <p>
 * Note that arbitrary conversion logic is always possible using {@link OnVersionChange &#64;OnVersionChange}.
 *
 * @see JField#upgradeConversion
 * @see Encoding#convert Encoding.convert()
 */
public enum UpgradeConversionPolicy {

    /**
     * Do not attempt to automatically convert values to the new type.
     *
     * <p>
     * Instead, during a schema version change, the field will be reset to the default value of the field's new type.
     */
    RESET(false, false),

    /**
     * Attempt automatic conversion of field values to the new type, and if automatic conversion fails,
     * set the value to the new type's default value as would {@link #RESET}.
     */
    ATTEMPT(true, false),

    /**
     * Attempt automatic conversion of field values to the new type, and if automatic conversion fails,
     * throw a {@link UpgradeConversionException}.
     */
    REQUIRE(true, true);

    private final boolean convertsValues;
    private final boolean requireConversion;

    UpgradeConversionPolicy(boolean convertsValues, boolean requireConversion) {
        this.convertsValues = convertsValues;
        this.requireConversion = requireConversion;
    }

    /**
     * Determine whether this policy should attempt to convert field values from the old type to the new type.
     *
     * <p>
     * If this is false, the field's value will be set to the new type's default value.
     * If this is true, the field's old value will be converted to the field's new type if possible;
     * if the conversion fails, the behavior depends on {@link #isRequireConversion}.
     *
     * @return true if under this policy conversion should be attempted
     */
    public boolean isConvertsValues() {
        return this.convertsValues;
    }

    /**
     * Determine whether failed attempts to convert a field's value from the old type to the new type should be fatal.
     *
     * <p>
     * If this is true, a failed conversion attempt results in a {@link UpgradeConversionException} being thrown.
     * If this is false, a failed conversion attempt results in the field being set to the new type's default value.
     *
     * @return true if under this policy conversion is mandatory
     */
    public boolean isRequireConversion() {
        return this.requireConversion;
    }
}
