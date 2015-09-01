
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb.spring;

import com.google.common.base.Preconditions;

import java.io.IOException;

import org.jsimpledb.kv.util.NavigableMapKVStore;
import org.springframework.http.HttpInputMessage;
import org.springframework.http.HttpOutputMessage;
import org.springframework.http.MediaType;
import org.springframework.http.converter.AbstractHttpMessageConverter;
import org.springframework.http.converter.HttpMessageNotReadableException;

/**
 * Spring {@link org.springframework.http.converter.HttpMessageConverter HttpMessageConverter}
 * capable of encoding and decoding a {@link NavigableMapKVStore}.
 *
 * <p>
 * This class can be used for a general purpose, schema-aware HTTP based RPC mechanism as follows:
 * <ul>
 *  <li>Create an empty {@link NavigableMapKVStore} to hold the object data</li>
 *  <li>Create an empty in-memory {@link org.jsimpledb.SnapshotJTransaction} backed by the {@link NavigableMapKVStore}
 *      via {@link org.jsimpledb.JSimpleDB#createSnapshotTransaction JSimpleDB.createSnapshotTransaction()}</li>
 *  <li>Populate the {@link org.jsimpledb.SnapshotJTransaction} with the objects you wish to transmit</li>
 *  <li>Use this class to serialize/deserialize the {@link NavigableMapKVStore} for transit</li>
 *  <li>On the receving side, recreate the {@link org.jsimpledb.SnapshotJTransaction} using the deserialized
 *      {@link NavigableMapKVStore} and
 *      {@link org.jsimpledb.JSimpleDB#createSnapshotTransaction JSimpleDB.createSnapshotTransaction()}</li>
 *  <li>Query for object data in the {@link org.jsimpledb.SnapshotJTransaction} using the usual methods,
 *      benefiting from index query support, automatic schema updates, etc.</li>
 * </ul>
 */
public class KVStoreHttpMessageConverter extends AbstractHttpMessageConverter<NavigableMapKVStore> {

    /**
     * Default MIME type supported by this instance: {@code application/x-jsimpledb-kvstore}.
     *
     * <p>
     * Can be overridden in the constructor.
     */
    public static final MediaType DEFAULT_MIME_TYPE = new MediaType("application", "x-jsimpledb-kvstore");

    /**
     * Construtor.
     *
     * <p>
     * Configures this instance for the {@link #DEFAULT_MIME_TYPE}.
     */
    public KVStoreHttpMessageConverter() {
        this(DEFAULT_MIME_TYPE);
    }

    /**
     * Construtor.
     *
     * @param mimeTypes supported MIME type(s)
     */
    public KVStoreHttpMessageConverter(MediaType... mimeTypes) {
        super(mimeTypes);
    }

// AbstractHttpMessageConverter

    @Override
    protected Long getContentLength(NavigableMapKVStore kvstore, MediaType contentType) {
        Preconditions.checkArgument(kvstore != null, "null kvstore");
        return kvstore.encodedLength();
    }

    @Override
    protected boolean supports(Class<?> clazz) {
        return clazz == NavigableMapKVStore.class;
    }

    @Override
    protected NavigableMapKVStore readInternal(Class<? extends NavigableMapKVStore> clazz, HttpInputMessage inputMessage)
      throws IOException {
        final NavigableMapKVStore kvstore;
        try {
            kvstore = NavigableMapKVStore.decode(inputMessage.getBody());
        } catch (IllegalArgumentException e) {
            throw new HttpMessageNotReadableException("invalid encoding key/value store", e);
        }
        return clazz.cast(kvstore);
    }

    @Override
    protected void writeInternal(NavigableMapKVStore kvstore, HttpOutputMessage outputMessage) throws IOException {
        Preconditions.checkArgument(kvstore != null, "null kvstore");
        kvstore.encode(outputMessage.getBody());
    }
}

