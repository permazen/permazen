
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.encoding;

import com.google.common.base.Preconditions;

import io.permazen.util.ByteReader;
import io.permazen.util.ByteWriter;

abstract class Concat2Encoding<T, S1, S2> extends BuiltinEncoding<T> {

    private static final long serialVersionUID = -7395218884659436172L;

    protected final Encoding<S1> type1;
    protected final Encoding<S2> type2;

    protected Concat2Encoding(Class<T> type, T defaultValue, Encoding<S1> type1, Encoding<S2> type2) {
       super(type, defaultValue);
       this.type1 = type1;
       this.type2 = type2;
    }

    protected Concat2Encoding(Class<T> type, Encoding<S1> type1, Encoding<S2> type2) {
       this(type, null, type1, type2);
    }

// Encoding

    @Override
    public T read(ByteReader reader) {
        Preconditions.checkArgument(reader != null);
        return this.join(this.type1.read(reader), this.type2.read(reader));
    }

    @Override
    public void write(ByteWriter writer, T value) {
        Preconditions.checkArgument(value != null, "null value");
        Preconditions.checkArgument(writer != null);
        this.type1.write(writer, this.split1(value));
        this.type2.write(writer, this.split2(value));
    }

    @Override
    public void skip(ByteReader reader) {
        Preconditions.checkArgument(reader != null);
        this.type1.skip(reader);
        this.type2.skip(reader);
    }

    @Override
    public int compare(T value1, T value2) {
        int diff = this.type1.compare(this.split1(value1), this.split1(value2));
        if (diff != 0)
            return diff;
        return this.type2.compare(this.split2(value1), this.split2(value2));
    }

    @Override
    public boolean hasPrefix0x00() {
        return this.type1.hasPrefix0x00();
    }

    @Override
    public boolean hasPrefix0xff() {
        return this.type1.hasPrefix0xff();
    }

    protected abstract T join(S1 value1, S2 value2);

    protected abstract S1 split1(T value);

    protected abstract S2 split2(T value);
}
