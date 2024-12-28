
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.core;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

import io.permazen.encoding.AbstractEncoding;
import io.permazen.encoding.Encoding;
import io.permazen.encoding.EncodingId;
import io.permazen.util.ByteData;
import io.permazen.util.UnsignedIntEncoder;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.OptionalInt;

/**
 * This is the inner, non-null supporting {@link Encoding} for {@link EnumValueEncoding}.
 *
 * <p>
 * Binary encoding is via the {@link UnsignedIntEncoder}-encoded {@linkplain EnumValue#getOrdinal ordinal} value.
 */
@SuppressWarnings("serial")
class EnumValueBaseEncoding extends AbstractEncoding<EnumValue> {

    private static final long serialVersionUID = -5645700883023141035L;

    private final Map<String, EnumValue> identifierMap;
    private final List<EnumValue> enumValueList;

// Constructors

    /**
     * Create an anonymous instance.
     *
     * @throws IllegalArgumentException if {@code idents} is null or invalid
     */
    EnumValueBaseEncoding(List<String> idents) {
        this(null, idents);
    }

    /**
     * Constructor.
     *
     * @param encodingId encoding ID, or null for an anonymous instance
     * @throws IllegalArgumentException if {@code idents} is null or invalid
     */
    EnumValueBaseEncoding(EncodingId encodingId, List<String> idents) {
        super(encodingId, EnumValue.class, null);
        this.identifierMap = Collections.unmodifiableMap(EnumValueEncoding.validateIdentifiers(idents));
        this.enumValueList = Collections.unmodifiableList(Lists.newArrayList(this.identifierMap.values()));
    }

    List<String> getIdentifiers() {
        return Collections.unmodifiableList(Lists.newArrayList(this.identifierMap.keySet()));
    }

// Encoding

    @Override
    public EnumValue read(ByteData.Reader reader) {
        Preconditions.checkArgument(reader != null);
        final int ordinal = UnsignedIntEncoder.read(reader);
        try {
            return this.enumValueList.get(ordinal);
        } catch (IndexOutOfBoundsException e) {
            throw new IllegalArgumentException(String.format(
              "enum ordinal %d not in the range [0..%d)", ordinal, this.enumValueList.size()), e);
        }
    }

    @Override
    public void write(ByteData.Writer writer, EnumValue value) {
        Preconditions.checkArgument(writer != null);
        UnsignedIntEncoder.write(writer, this.validate(value).getOrdinal());
    }

    @Override
    public void skip(ByteData.Reader reader) {
        Preconditions.checkArgument(reader != null);
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
            throw new IllegalArgumentException(String.format("unknown enum identifier \"%s\"", string));
        return value;
    }

    @Override
    public int compare(EnumValue value1, EnumValue value2) {
        return Integer.compare(value1.getOrdinal(), value2.getOrdinal());
    }

    @Override
    public boolean supportsNull() {
        return false;
    }

    @Override
    public boolean sortsNaturally() {
        return false;
    }

    @Override
    public boolean hasPrefix0x00() {
        return true;
    }

    @Override
    public boolean hasPrefix0xff() {
        return false;
    }

    @Override
    public OptionalInt getFixedWidth() {
        return OptionalInt.empty();
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
            throw new IllegalArgumentException(String.format(
              "enum value %s has incorrect ordinal value %d != %d", value, ordinal, sameName.getOrdinal()));
        }
        throw new IllegalArgumentException(String.format("unknown enum value %s", value));
    }

// Object

    @Override
    public int hashCode() {
        return super.hashCode() ^ this.enumValueList.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this)
            return true;
        if (!super.equals(obj))
            return false;
        final EnumValueBaseEncoding that = (EnumValueBaseEncoding)obj;
        return this.enumValueList.equals(that.enumValueList);
    }
}
