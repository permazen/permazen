
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.spring;

import com.google.common.base.Preconditions;

import io.permazen.DetachedPermazenTransaction;
import io.permazen.Permazen;
import io.permazen.PermazenObject;
import io.permazen.ValidationException;
import io.permazen.ValidationMode;
import io.permazen.kv.util.MemoryKVStore;

import jakarta.validation.groups.Default;

import java.io.IOException;

import org.dellroad.stuff.validation.ValidationUtil;
import org.springframework.http.HttpInputMessage;
import org.springframework.http.HttpOutputMessage;
import org.springframework.http.MediaType;
import org.springframework.http.converter.AbstractHttpMessageConverter;
import org.springframework.http.converter.HttpMessageNotReadableException;

/**
 * Spring {@link org.springframework.http.converter.HttpMessageConverter HttpMessageConverter} capable of
 * encoding and decoding a graph of {@link PermazenObject}s contained in a {@link DetachedPermazenTransaction} that is
 * backed by a {@link MemoryKVStore}.
 *
 * <p>
 * The MIME type used is {@code application/x-permazen-transaction}.
 *
 * <p>
 * Validation of all incoming objects is supported; see {@link #setValidationGroups setValidationGroups()}.
 *
 * @see PermazenObjectHttpMessageConverter
 * @see KVStoreHttpMessageConverter
 */
public class DetachedPermazenTransactionHttpMessageConverter extends AbstractHttpMessageConverter<DetachedPermazenTransaction> {

    /**
     * MIME type supported by this class: {@code application/x-permazen-transaction}.
     *
     * <p>
     * Can be overridden in the constructor.
     */
    public static final MediaType MIME_TYPE = new MediaType("application", "x-permazen-transaction");

    static final MediaType LEGACY_MIME_TYPE = new MediaType("application", "x-jsimpledb-transaction");

    private final Permazen jdb;

    private Class<?>[] validationGroups;

    /**
     * Constructor.
     *
     * @param jdb {@link Permazen} instance defining the convertible types
     */
    public DetachedPermazenTransactionHttpMessageConverter(Permazen jdb) {
        this(jdb, MIME_TYPE, LEGACY_MIME_TYPE);
    }

    /**
     * Constructor.
     *
     * @param jdb {@link Permazen} instance defining the convertible types
     * @param supportedMediaTypes supported media types
     */
    public DetachedPermazenTransactionHttpMessageConverter(Permazen jdb, MediaType... supportedMediaTypes) {
        super(supportedMediaTypes);
        Preconditions.checkArgument(jdb != null, "null jdb");
        this.jdb = jdb;
    }

    /**
     * Set validation groups used to validate all incoming objects.
     *
     * <p>
     * If set to null, no validation is performed. Otherwise, all incoming objects in the transaction are validated,
     * using the specified validation groups, or {@link Default} if an empty is specified.
     *
     * <p>
     * By default, this is null.
     *
     * @param groups validation group(s) to use for validation; if empty, {@link Default} is assumed;
     *  if null, no validation is performed
     */
    public void setValidationGroups(Class<?>... groups) {
        this.validationGroups = groups;
    }

// AbstractHttpMessageConverter

    @Override
    protected Long getContentLength(DetachedPermazenTransaction jtx, MediaType contentType) {
        return KVStoreHttpMessageConverter.getKVStoreContentLength(jtx.getTransaction().getKVStore());
    }

    @Override
    protected boolean supports(Class<?> clazz) {
        return clazz == DetachedPermazenTransaction.class;
    }

    @Override
    protected DetachedPermazenTransaction readInternal(Class<? extends DetachedPermazenTransaction> clazz, HttpInputMessage input)
      throws IOException {
        return clazz.cast(DetachedPermazenTransactionHttpMessageConverter.readDetachedTransaction(
          this.jdb, input, this.validationGroups));
    }

    @Override
    protected void writeInternal(DetachedPermazenTransaction jtx, HttpOutputMessage output) throws IOException {
        KVStoreHttpMessageConverter.writeKVStore(jtx.getTransaction().getKVStore(), output);
    }

// Utility methods

    static DetachedPermazenTransaction readDetachedTransaction(Permazen jdb, HttpInputMessage input, Class<?>[] validationGroups)
      throws IOException {

        // Decode key/value store
        final MemoryKVStore kvstore = new MemoryKVStore();
        KVStoreHttpMessageConverter.readKVStore(kvstore, input);

        // Create detached transaction
        final DetachedPermazenTransaction jtx = jdb.createDetachedTransaction(kvstore,
          validationGroups != null ? ValidationMode.MANUAL : ValidationMode.DISABLED);

        // Optionally validate
        if (validationGroups != null) {
            for (PermazenObject jobj : jtx.getAll(PermazenObject.class))
                jobj.revalidate(validationGroups);
            try {
                jtx.validate();
            } catch (ValidationException e) {
                throw new HttpMessageNotReadableException("incoming object graph failed validation: "
                  + ValidationUtil.describe(e.getViolations()), input);
            }
        }

        // Done
        return jtx;
    }
}
