
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.core;

/**
 * Default {@link FieldType} for {@link Enum} types. Null values are supported.
 *
 * <p>
 * Enums present a problem because they have both a {@link Enum#name name()} and an {@link Enum#ordinal ordinal()} value,
 * and either or both of these might change over time (perhaps due to some code refactoring). Therefore,
 * it's possible to encounter an unknown value when decoding an enum constant. To gracefully handle these situations,
 * the binary encoding includes both the constant's {@link Enum#ordinal ordinal()}s and its {@link Enum#name name()},
 * and when decoding an enum constant the following logic applies:
 * <ol>
 *  <li>If the name matches an enum constant, that constant is returned, regardless of whether the ordinal matches or not</li>
 *  <li>If the name does not match but the ordinal matches an enum constant, that constant is returned</li>
 *  <li>Otherwise, neither the name nor the ordinal matches, and null is returned</li>
 * </ol>
 * </p>
 *
 * <p>
 * As with all {@link FieldType}s, a separate string encoding is also supported for enum values: this is just the enum name
 * surrounded by square brackets. If an unrecognized name is encountered when {@linkplain #fromString decoding
 * from a string}, null is returned.
 * </p>
 *
 */
public class EnumType<T extends Enum<T>> extends NullSafeType<T> {

    private final IntegerType intType = new IntegerType();
    private final StringType stringType = new StringType();

    /**
     * Constructor.
     *
     * @throws ClassCastException if {@code type} is not an {@link Enum}
     */
    EnumType(Class<T> type) {
        super(new RawEnumType<T>(type));
    }
}

