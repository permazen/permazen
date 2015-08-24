
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb.core;

import com.google.common.base.Function;
import com.google.common.collect.Lists;
import com.google.common.reflect.TypeToken;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.dellroad.stuff.java.EnumUtil;
import org.jsimpledb.parse.ParseContext;
import org.jsimpledb.util.ByteReader;
import org.jsimpledb.util.ByteWriter;
import org.jsimpledb.util.UnsignedIntEncoder;

/**
 * This is the inner, non-null supporting {@link FieldType} for {@link EnumFieldType}.
 */
class EnumType extends NonNullFieldType<EnumValue> {

    final Class<? extends Enum<?>> enumType;
    final Map<String, EnumValue> identifierMap;
    final List<EnumValue> enumValueList;

    EnumType(Class<? extends Enum<?>> enumType, String typeName, List<String> idents) {
        super(typeName, TypeToken.of(EnumValue.class), 0);
        this.enumType = enumType;
        this.identifierMap = Collections.unmodifiableMap(EnumFieldType.validateIdentifiers(idents));
        this.enumValueList = Collections.unmodifiableList(Lists.newArrayList(this.identifierMap.values()));
    }

    List<String> getIdentifiers() {
        return Collections.unmodifiableList(Lists.newArrayList(this.identifierMap.keySet()));
    }

    Class<? extends Enum<?>> getEnumType() {
        return this.enumType;
    }

// FieldType

    @Override
    public EnumValue read(ByteReader reader) {
        final int ordinal = UnsignedIntEncoder.read(reader);
        try {
            return this.enumValueList.get(ordinal);
        } catch (IndexOutOfBoundsException e) {
            throw new IllegalArgumentException("enum ordinal " + ordinal + " not in the range [0.."
              + this.enumValueList.size() + ")", e);
        }
    }

    @Override
    public void write(ByteWriter writer, EnumValue value) {
        UnsignedIntEncoder.write(writer, this.validate(value).getOrdinal());
    }

    @Override
    public void skip(ByteReader reader) {
        UnsignedIntEncoder.skip(reader);
    }

    @Override
    public String toString(EnumValue value) {
        return this.validate(value).getName();
    }

    @Override
    public EnumValue fromString(String string) {
        final EnumValue value = this.identifierMap.get(string);
        if (value == null)
            throw new IllegalArgumentException("unknown identifier `" + string + "' for enum type `" + this.getName() + "'");
        return value;
    }

    @Override
    public String toParseableString(EnumValue value) {
        return this.toString(value);
    }

    @Override
    public EnumValue fromParseableString(ParseContext context) {
        final Matcher matcher = context.tryPattern(Pattern.compile(EnumFieldType.IDENT_PATTERN));
        if (matcher == null)
            throw new IllegalArgumentException("invalid enum identifier");
        final String ident = matcher.group();
        return this.fromString(ident);
    }

    @Override
    public int compare(EnumValue value1, EnumValue value2) {
        return Integer.compare(value1.getOrdinal(), value2.getOrdinal());
    }

    @Override
    public boolean hasPrefix0xff() {
        return false;
    }

    @Override
    public EnumValue validate(Object obj) {
        final EnumValue value = super.validate(obj);
        final String name = value.getName();
        final int ordinal = value.getOrdinal();
        EnumValue sameOrdinal;
        try {
            sameOrdinal = this.enumValueList.get(ordinal);
        } catch (IndexOutOfBoundsException e) {
            sameOrdinal = null;
        }
        if (sameOrdinal != null && sameOrdinal.getName().equals(name))
            return value;
        final EnumValue sameName = this.identifierMap.get(name);
        if (sameName != null) {
            throw new IllegalArgumentException("enum value " + value + " has incorrect ordinal value "
              + ordinal + " != " + sameName.getOrdinal());
        }
        throw new IllegalArgumentException("unknown enum value " + value);
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

// Object

    @Override
    public int hashCode() {
        return super.hashCode() ^ (this.enumType != null ? this.enumType.hashCode() : 0) ^ this.enumValueList.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this)
            return true;
        if (!super.equals(obj))
            return false;
        final EnumType that = (EnumType)obj;
        return this.enumType == that.enumType && this.enumValueList.equals(that.enumValueList);
    }
}

