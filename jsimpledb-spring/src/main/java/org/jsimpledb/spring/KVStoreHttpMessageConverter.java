
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb.spring;

import com.google.common.base.Preconditions;

import java.io.IOException;
import java.util.Iterator;

import org.jsimpledb.kv.KVPair;
import org.jsimpledb.kv.KVStore;
import org.jsimpledb.kv.util.KeyListEncoder;
import org.jsimpledb.kv.util.NavigableMapKVStore;
import org.jsimpledb.util.CloseableIterator;
import org.springframework.http.HttpInputMessage;
import org.springframework.http.HttpOutputMessage;
import org.springframework.http.MediaType;
import org.springframework.http.converter.AbstractHttpMessageConverter;
import org.springframework.http.converter.HttpMessageNotReadableException;

/**
 * Spring {@link org.springframework.http.converter.HttpMessageConverter HttpMessageConverter}
 * capable of encoding and decoding a {@link KVStore}. The {@link KeyListEncoder} class is used
 * to compress common prefixes of consecutive keys.
 *
 * <p>
 * See {@link JObjectHttpMessageConverter} for a higher level API.
 *
 * @see SnapshotJTransactionHttpMessageConverter
 * @see JObjectHttpMessageConverter
 */
public class KVStoreHttpMessageConverter extends AbstractHttpMessageConverter<KVStore> {

    /**
     * Default MIME type supported by this instance: {@code application/x-jsimpledb-kvstore}.
     */
    public static final MediaType DEFAULT_MIME_TYPE = new MediaType("application", "x-jsimpledb-kvstore");

    /**
     * Constructor.
     *
     * <p>
     * Configures this instance for the {@link #DEFAULT_MIME_TYPE}.
     */
    public KVStoreHttpMessageConverter() {
        super(DEFAULT_MIME_TYPE);
    }

    /**
     * Constructor.
     *
     * @param mimeTypes supported MIME type(s)
     */
    public KVStoreHttpMessageConverter(MediaType... mimeTypes) {
        super(mimeTypes);
    }

// AbstractHttpMessageConverter

    @Override
    protected Long getContentLength(KVStore kvstore, MediaType mediaType) {
        return KVStoreHttpMessageConverter.getKVStoreContentLength(kvstore);
    }

    @Override
    protected boolean supports(Class<?> clazz) {
        return clazz == KVStore.class || clazz == NavigableMapKVStore.class;
    }

    @Override
    protected KVStore readInternal(Class<? extends KVStore> clazz, HttpInputMessage input) throws IOException {
        final NavigableMapKVStore kvstore = new NavigableMapKVStore();
        KVStoreHttpMessageConverter.readKVStore(kvstore, input);
        return clazz.cast(kvstore);
    }

    @Override
    protected void writeInternal(KVStore kvstore, HttpOutputMessage output) throws IOException {
        KVStoreHttpMessageConverter.writeKVStore(kvstore, output);
    }

// Utility methods

    /**
     * Determine the content length of the given key/value store when encoded as payload.
     *
     * @param kvstore key/value store to encode
     * @return payload content length
     * @throws IllegalArgumentException if {@code kvstore} is null
     */
    public static Long getKVStoreContentLength(KVStore kvstore) {
        Preconditions.checkArgument(kvstore != null, "null kvstore");
        try (CloseableIterator<KVPair> i = kvstore.getRange(null, null)) {
            return KeyListEncoder.writePairsLength(i);
        }
    }

    /**
     * Decode a key/value store HTTP payload.
     *
     * @param kvstore key/value store to populate from input
     * @param input HTTP payload input
     * @throws HttpMessageNotReadableException if {@code input} contains invalid content
     * @throws IOException if an I/O error occurs
     * @throws IllegalArgumentException if either parameter is null
     */
    public static void readKVStore(KVStore kvstore, HttpInputMessage input) throws IOException {
        Preconditions.checkArgument(kvstore != null, "null kvstore");
        Preconditions.checkArgument(input != null, "null input");
        try {
            for (Iterator<KVPair> i = KeyListEncoder.readPairs(input.getBody()); i.hasNext(); ) {
                final KVPair kv = i.next();
                kvstore.put(kv.getKey(), kv.getValue());
            }
        } catch (IllegalArgumentException e) {
            throw new HttpMessageNotReadableException("invalid endoded key/value store", e);
        } catch (RuntimeException e) {
            if (e.getCause() instanceof IOException)
                throw (IOException)e.getCause();
            throw e;
        }
    }

    /**
     * Encode a key/value store HTTP payload.
     *
     * @param kvstore key/value store to encode
     * @param output HTTP payload output
     * @throws IOException if an I/O error occurs
     * @throws IllegalArgumentException if either parameter is null
     */
    public static void writeKVStore(KVStore kvstore, HttpOutputMessage output) throws IOException {
        Preconditions.checkArgument(kvstore != null, "null kvstore");
        Preconditions.checkArgument(output != null, "null output");
        try (CloseableIterator<KVPair> i = kvstore.getRange(null, null)) {
            KeyListEncoder.writePairs(i, output.getBody());
        }
    }
}

