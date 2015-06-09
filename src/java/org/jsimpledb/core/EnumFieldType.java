
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb.core;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.dellroad.stuff.java.EnumUtil;

/**
 * The {@link FieldType} for {@link EnumField}s.
 *
 * <p>
 * Each {@link EnumFieldType} instance has an ordered list of {@link String} identifiers; two {@link EnumFieldType}
 * instances are not compatible unless they have identical identifier lists. The identifiers must be valid Java identifiers.
 * </p>
 *
 * <p>
 * Null values are supported by this class.
 * </p>
 */
public class EnumFieldType extends NullSafeType<EnumValue> {

    public static final String IDENT_PATTERN = "\\p{javaJavaIdentifierStart}\\p{javaJavaIdentifierPart}*";

    /**
     * Constructor to use when there is an associated {@link Enum} type;
     * {@link #getEnumType} will return {@code enumType}.
     *
     * @param enumType Java {@link Enum} type from which to derive the ordered identifier list
     * @param <T> enum type
     * @throws NullPointerException if {@code enumType} is null
     */
    public <T extends Enum<T>> EnumFieldType(Class<T> enumType) {
        super(new EnumType(enumType, enumType.getName(), EnumFieldType.getIdentifiers(enumType)));
    }

    /**
     * Constructor to use when there is no corresponding {@link Enum} type;
     * {@link #getEnumType} will return null.
     *
     * @param name name of this type
     * @param idents ordered list of identifiers
     * @throws IllegalArgumentException if {@code name} is null or invalid
     * @throws IllegalArgumentException if {@code idents} is null or contains a duplicate or invalid identifier
     */
    public EnumFieldType(String name, List<String> idents) {
        super(new EnumType(null, name, idents));
    }

    /**
     * Get the identifiers associated with this instance in ordinal order.
     *
     * @return unmodifiable, ordinally ordered list of identifiers
     */
    public List<String> getIdentifiers() {
        return ((EnumType)this.inner).getIdentifiers();
    }

    /**
     * Get the {@link Enum} type associated with this instance, if known.
     *
     * @return associated {@link Enum} type, or null if actual {@link Enum} type is unknown
     */
    public Class<? extends Enum<?>> getEnumType() {
        return ((EnumType)this.inner).getEnumType();
    }

    /**
     * Create an {@link EnumFieldType} instance suitable for use with the given {@link Enum} type.
     *
     * @param enumType Java {@link Enum} type from which to derive the ordered identifier list
     * @return an {@link EnumFieldType} based on {@code enumType}
     * @throws ClassCastException if {@code enumType} does not subclass {@link Enum}
     * @throws NullPointerException if {@code enumType} is null
     */
    public static EnumFieldType create(Class<?> enumType) {
        return EnumFieldType.doCreate(enumType);
    }

    // This method exists solely to bind the generic type parameters
    @SuppressWarnings("unchecked")
    private static <T extends Enum<T>> EnumFieldType doCreate(Class<?> enumType) {
        return new EnumFieldType((Class<T>)enumType.asSubclass(Enum.class));
    }

    /**
     * Create an {@link EnumFieldType} instance suitable for use with the given identifier list.
     *
     * <p>
     * If {@code name} is the name of an {@link Enum} class with matching identifiers, then it will
     * associated with the created instance and returned by its {@link #getEnumType} method.
     * </p>
     *
     * @param name name of the type
     * @param idents ordered list of identifiers
     * @return an {@link EnumFieldType} based on the specified {@code idents}
     * @throws IllegalArgumentException if {@code name} is null or invalid
     * @throws IllegalArgumentException if {@code idents} is null or contains a duplicate or invalid identifier
     */
    public static EnumFieldType create(String name, List<String> idents) {
        return EnumFieldType.doCreate(name, idents);
    }

    // This method exists solely to bind the generic type parameters
    @SuppressWarnings("unchecked")
    private static <T extends Enum<T>> EnumFieldType doCreate(String name, List<String> idents) {
        do {

            // Search for Enum type
            final Class<T> enumType;
            try {
                enumType = (Class<T>)Class.forName(name, false,
                  Thread.currentThread().getContextClassLoader()).asSubclass(Enum.class);
            } catch (Exception e) {
                break;
            }

            // Validate it has the expected identifier list
            if (!EnumFieldType.getIdentifiers(enumType).equals(idents))
                break;

            // We're good
            return new EnumFieldType(enumType);
        } while (false);

        // Enum type not found or has incompatible identifier list
        return new EnumFieldType(name, idents);
    }

    /**
     * Validate a list of enum identifiers and build a mapping from identifier to corresponding {@link EnumValue}.
     * The returned mapping will iterate the {@link EnumValue}s in ordinal order.
     *
     * @param idents enum identifiers
     * @return ordinally ordered mapping from identifier to {@link EnumValue}
     * @throws IllegalArgumentException if {@code idents} is null
     * @throws IllegalArgumentException if any identifier in {@code idents} is null, duplicate, or not a valid Java identifier
     */
    public static Map<String, EnumValue> validateIdentifiers(List<String> idents) {
        Preconditions.checkArgument(idents != null, "null idents");
        final LinkedHashMap<String, EnumValue> identifierMap = idents instanceof Collection ?
          new LinkedHashMap<String, EnumValue>(((Collection<?>)idents).size()) : new LinkedHashMap<String, EnumValue>();
        for (String ident : idents) {
            final int index = identifierMap.size();
            Preconditions.checkArgument(ident != null, "invalid null enum identifier at index " + index);
            Preconditions.checkArgument(!ident.equals("null") && ident.matches(IDENT_PATTERN),
              "invalid enum identifier `" + ident + "' at index " + index);
            final EnumValue otherValue = identifierMap.put(ident, new EnumValue(ident, index));
            if (otherValue != null) {
                throw new IllegalArgumentException("invalid duplicate enum identifier `" + ident
                  + "' at indexes " + otherValue.getOrdinal() + " and " + index);
            }
        }
        return identifierMap;
    }

    @SuppressWarnings("unchecked")
    private static <T extends Enum<T>> List<String> getIdentifiers(Class<T> enumType) {
        return Lists.transform(EnumUtil.getValues(enumType), new Function<T, String>() {
            @Override
            public String apply(T value) {
                return value.name();
            }
        });
    }
}

