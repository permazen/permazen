
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.kv.spanner;

import com.google.cloud.ByteArray;

import io.permazen.util.ByteData;

import java.io.IOException;
import java.io.InputStream;

/**
 * Spanner utility methods.
 */
public final class Util {

    private Util() {
    }

    /**
     * Convert {@link ByteData} to {@link ByteArray}.
     *
     * @param data input {@link ByteData}, or null
     * @return equivalent {@link ByteArray}, or null if {@code data} is null
     */
    public static ByteArray wrap(ByteData data) {
        if (data == null)
            return null;
        try (InputStream input = data.newReader()) {
            return ByteArray.copyFrom(input);
        } catch (IOException e) {
            throw new RuntimeException("unexpected error", e);
        }
    }

    /**
     * Convert {@link ByteArray} to {@link ByteData}.
     *
     * @param data input {@link ByteArray}, or null
     * @return equivalent {@link ByteData}, or null if {@code data} is null
     */
    public static ByteData unwrap(ByteArray data) {
        if (data == null)
            return null;
        final ByteData.Writer writer = ByteData.newWriter(data.length());
        try (InputStream input = data.asInputStream()) {
            input.transferTo(writer);
        } catch (IOException e) {
            throw new RuntimeException("unexpected error", e);
        }
        return writer.toByteData();
    }
}
