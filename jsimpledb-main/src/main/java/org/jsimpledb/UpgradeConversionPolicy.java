
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb;

/**
 * Policies to apply when a simple field's type changes during a schema update.
 *
 * <p>
 * <b>Type Changes</b>
 *
 * <p>
 * JSimpleDB fields are identified by their {@linkplain JSimpleField#getStorageId storage ID's}, which is typically
 * {@linkplain StorageIdGenerator derived automatically} from the field's name.
 * With some restrictions<sup>*</sup>, the type of a field may change between schema versions; this includes both
 * regular simple fields (e.g., changing from {@code int} to {@link String}) and sub-fields of complex fields
 * (e.g., changing from {@link java.util.List List&lt;Integer&gt;} to {@link java.util.List List&lt;String&gt;}).
 *
 * <p>
 * When upgrading an object's schema version, JSimpleDB supports optional automatic conversion of a field's value from its
 * old type to its new type. For example, an {@code int} field value {@code 1234} would become the {@link String} field value
 * {@code "1234"}. How exactly this conversion is performed is defined by the field's new {@link org.jsimpledb.core.FieldType};
 * see {@link org.jsimpledb.core.FieldType#convert FieldType.convert()} for details.
 *
 * <p>
 * This class is used to {@linkplain org.jsimpledb.annotation.JField#upgradeConversion specify} whether such automatic
 * conversion should occur when a simple field's type changes, and if so, whether the conversion must always succeed.
 *
 * <p>
 * <sup>*</sup>A simple field may not have different types across schema versions and be indexed in both versions.
 *
 * <p>
 * <b>References and Enums</b>
 *
 * <p>
 * JSimpleDB considers {@link Enum} types with different identifier lists as different types. However, automatic
 * conversion of {@link Enum} values will work if the existing value's name is valid for the new {@link Enum} type.
 *
 * <p>
 * Automatic conversion of reference fields also works as long as the referenced object's type is assignable
 * to the field's new Java type (otherwise, the field is set to null).
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
 * Note that arbitrary conversion logic is always possible using
 * {@link org.jsimpledb.annotation.OnVersionChange &#64;OnVersionChange}.
 *
 * @see org.jsimpledb.annotation.JField#upgradeConversion
 * @see org.jsimpledb.core.FieldType#convert FieldType.convert()
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
     * Attempt automatic conversion of field values to the new type, and iIf automatic conversion fails,
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

