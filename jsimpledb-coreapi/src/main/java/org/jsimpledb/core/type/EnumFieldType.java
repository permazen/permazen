
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb.core.type;

import com.google.common.base.Preconditions;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.dellroad.stuff.java.EnumUtil;
import org.jsimpledb.core.EnumValue;

/**
 * The {@link org.jsimpledb.core.FieldType} for {@link EnumValue}, which is the data type used by
 * {@link org.jsimpledb.core.EnumField}.
 *
 * <p>
 * Each {@link EnumFieldType} instance has an ordered list of {@link String} identifiers; two {@link EnumFieldType}
 * instances are not compatible unless they have identical identifier lists. The identifiers must be valid Java identifiers.
 *
 * <p>
 * Note that whatever {@link Enum} type may be used to represent values at the Java layer is unimportant;
 * only the ordered list of identifiers matters.
 *
 * <p>
 * Null values are supported by this class.
 */
public class EnumFieldType extends NullSafeType<EnumValue> {

    public static final String IDENT_PATTERN = "\\p{javaJavaIdentifierStart}\\p{javaJavaIdentifierPart}*";

    private static final long serialVersionUID = -968533056184967301L;

    /**
     * Constructor that derives the type name and identifier list from the given {@link Enum} type.
     *
     * @param enumType Java {@link Enum} type from which to derive type name and ordered identifier list
     * @param <T> enum type
     * @throws NullPointerException if {@code enumType} is null
     */
    public <T extends Enum<T>> EnumFieldType(Class<T> enumType) {
        this(EnumFieldType.getIdentifiers(enumType));
    }

    /**
     * Primary constructor.
     *
     * @param idents ordered list of identifiers
     * @throws IllegalArgumentException if {@code name} is null or invalid
     * @throws IllegalArgumentException if {@code idents} is null or contains a duplicate or invalid identifier
     */
    public EnumFieldType(List<String> idents) {
        super(new EnumType(idents));
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
        final LinkedHashMap<String, EnumValue> identifierMap = new LinkedHashMap<>(idents.size());
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

    @SuppressWarnings({
        "unchecked",
        "rawtypes"      // https://bugs.openjdk.java.net/browse/JDK-8012685
    })
    private static <T extends Enum<T>> List<String> getIdentifiers(Class<T> enumType) {
        return EnumUtil.getValues(enumType).stream()
          .map(Enum::name)
          .collect(Collectors.toList());
    }
}

