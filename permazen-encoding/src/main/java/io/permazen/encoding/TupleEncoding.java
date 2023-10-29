
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.encoding;

import com.google.common.base.Preconditions;
import com.google.common.reflect.TypeToken;

import io.permazen.tuple.Tuple;
import io.permazen.util.ByteReader;
import io.permazen.util.ByteWriter;
import io.permazen.util.ParseContext;

import java.util.Arrays;
import java.util.List;

/**
 * Superclass for {@link Encoding}s created from the concatenation of other {@link Encoding}s.
 */
public abstract class TupleEncoding<T extends Tuple> extends AbstractEncoding<T> {

    private static final long serialVersionUID = 8691368371643936848L;

    final List<Encoding<?>> encodings;
    final int size;

// Constructors

    /**
     * Create an anonymous instance.
     *
     * @param typeToken this encoding's composite value type
     * @param encodings encodings to concatenate
     * @throws IllegalArgumentException if {@code typeToken} or {@code encodings} is null
     */
    protected TupleEncoding(TypeToken<T> typeToken, Encoding<?>... encodings) {
        this(null, typeToken, encodings);
    }

    /**
     * Constructor.
     *
     * @param encodingId encoding ID, or null for an anonymous instance
     * @param typeToken this encoding's composite value type
     * @param encodings encodings to concatenate
     * @throws IllegalArgumentException if {@code typeToken} or {@code encodings} is null
     */
    protected TupleEncoding(EncodingId encodingId, TypeToken<T> typeToken, Encoding<?>... encodings) {
        super(encodingId, typeToken, null);
        Preconditions.checkArgument(encodings != null, "null encodings");
        this.encodings = Arrays.<Encoding<?>>asList(encodings);
        this.size = this.encodings.size();
    }

// Public Methods

    /**
     * Get the number of component encodings in this encoding.
     *
     * @return number of component encodings
     */
    public int getSize() {
        return this.size;
    }

// Encoding

    @Override
    public T read(ByteReader reader) {
        final Object[] values = new Object[this.size];
        for (int i = 0; i < this.size; i++)
            values[i] = this.encodings.get(i).read(reader);
        return this.createTuple(values);
    }

    @Override
    public void write(ByteWriter writer, T tuple) {
        final List<Object> values = this.asList(tuple);
        for (int i = 0; i < this.size; i++)
            this.encodings.get(i).validateAndWrite(writer, values.get(i));
    }

    @Override
    public void skip(ByteReader reader) {
        for (Encoding<?> encoding : this.encodings)
            encoding.skip(reader);
    }

    @Override
    public String toParseableString(T tuple) {
        final List<Object> values = this.asList(tuple);
        final StringBuilder buf = new StringBuilder();
        buf.append('[');
        for (int i = 0; i < this.size; i++) {
            if (i > 0)
                buf.append(',');
            buf.append(this.toParseableString(this.encodings.get(i), values.get(i)));
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
            values[i] = this.encodings.get(i).fromParseableString(context);
        }
        context.expect(']');
        return this.createTuple(values);
    }

    @Override
    public int compare(Tuple tuple1, Tuple tuple2) {
        final List<Object> values1 = this.asList(tuple1);
        final List<Object> values2 = this.asList(tuple2);
        for (int i = 0; i < this.size; i++) {
            final int diff = this.compare(this.encodings.get(i), values1.get(i), values2.get(i));
            if (diff != 0)
                return diff;
        }
        return 0;
    }

    @Override
    public boolean hasPrefix0xff() {
        return this.encodings.get(0).hasPrefix0xff();
    }

    @Override
    public boolean hasPrefix0x00() {
        return this.encodings.get(0).hasPrefix0x00();
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
    private <T> String toParseableString(Encoding<T> encoding, Object obj) {
        return encoding.toParseableString(encoding.validate(obj));
    }

    // This method exists solely to bind the generic type parameters
    private <T> int compare(Encoding<T> encoding, Object obj1, Object obj2) {
        return encoding.compare(encoding.validate(obj1), encoding.validate(obj2));
    }
}
