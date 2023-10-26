
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.core.type;

import com.google.common.base.Preconditions;
import com.google.common.reflect.TypeToken;

import io.permazen.core.FieldType;
import io.permazen.tuple.Tuple;
import io.permazen.util.ByteReader;
import io.permazen.util.ByteWriter;
import io.permazen.util.ParseContext;

import java.util.Arrays;
import java.util.List;

/**
 * Superclass for {@link FieldType}s created from the concatenation of other {@link FieldType}s.
 *
 * <p>
 * Binary encoding is via the concatenation of the individual element encodings.
 *
 * <p>
 * Instances are {@linkplain FieldType#getEncodingId anonymous}.
 */
public abstract class TupleFieldType<T extends Tuple> extends NonNullFieldType<T> {

    private static final long serialVersionUID = 8691368371643936848L;

    final List<FieldType<?>> fieldTypes;
    final int size;

    protected TupleFieldType(TypeToken<T> typeToken, FieldType<?>... fieldTypes) {
        super(null, typeToken);
        this.fieldTypes = Arrays.<FieldType<?>>asList(fieldTypes);
        this.size = this.fieldTypes.size();
    }

    public int getSize() {
        return this.size;
    }

    @Override
    public T read(ByteReader reader) {
        final Object[] values = new Object[this.size];
        for (int i = 0; i < this.size; i++)
            values[i] = this.fieldTypes.get(i).read(reader);
        return this.createTuple(values);
    }

    @Override
    public void write(ByteWriter writer, T tuple) {
        final List<Object> values = this.asList(tuple);
        for (int i = 0; i < this.size; i++)
            this.fieldTypes.get(i).validateAndWrite(writer, values.get(i));
    }

    @Override
    public void skip(ByteReader reader) {
        for (FieldType<?> fieldType : this.fieldTypes)
            fieldType.skip(reader);
    }

    @Override
    public String toParseableString(T tuple) {
        final List<Object> values = this.asList(tuple);
        final StringBuilder buf = new StringBuilder();
        buf.append('[');
        for (int i = 0; i < this.size; i++) {
            if (i > 0)
                buf.append(',');
            buf.append(this.toParseableString(this.fieldTypes.get(i), values.get(i)));
        }
        buf.append(']');
        return buf.toString();
    }

    @Override
    public T fromParseableString(ParseContext context) {
        context.expect('[');
        final Object[] values = new Object[this.size];
        for (int i = 0; i < this.size; i++) {
            if (i > 0)
                context.expect(',');
            values[i] = this.fieldTypes.get(i).fromParseableString(context);
        }
        context.expect(']');
        return this.createTuple(values);
    }

    @Override
    public int compare(Tuple tuple1, Tuple tuple2) {
        final List<Object> values1 = this.asList(tuple1);
        final List<Object> values2 = this.asList(tuple2);
        for (int i = 0; i < this.size; i++) {
            final int diff = this.compare(this.fieldTypes.get(i), values1.get(i), values2.get(i));
            if (diff != 0)
                return diff;
        }
        return 0;
    }

    @Override
    public boolean hasPrefix0xff() {
        return this.fieldTypes.get(0).hasPrefix0xff();
    }

    @Override
    public boolean hasPrefix0x00() {
        return this.fieldTypes.get(0).hasPrefix0x00();
    }

// Internal methods

    protected abstract T createTuple(Object[] values);

    private List<Object> asList(Tuple tuple) {
        Preconditions.checkArgument(tuple != null, "null tuple");
        final List<Object> list = tuple.asList();
        if (list.size() != this.size)
            throw new IllegalArgumentException("tuple has the wrong cardinality " + list.size() + " != " + this.size);
        return list;
    }

    // This method exists solely to bind the generic type parameters
    private <T> String toParseableString(FieldType<T> fieldType, Object obj) {
        return fieldType.toParseableString(fieldType.validate(obj));
    }

    // This method exists solely to bind the generic type parameters
    private <T> int compare(FieldType<T> fieldType, Object obj1, Object obj2) {
        return fieldType.compare(fieldType.validate(obj1), fieldType.validate(obj2));
    }
}
