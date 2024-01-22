
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.spring;

import com.google.common.base.Preconditions;

import io.permazen.DetachedPermazenTransaction;
import io.permazen.Permazen;
import io.permazen.PermazenObject;
import io.permazen.core.ObjId;

import jakarta.validation.groups.Default;

import java.io.IOException;
import java.util.Collections;

import org.springframework.http.HttpInputMessage;
import org.springframework.http.HttpOutputMessage;
import org.springframework.http.MediaType;
import org.springframework.http.converter.AbstractHttpMessageConverter;
import org.springframework.http.converter.HttpMessageNotReadableException;

/**
 * Spring {@link org.springframework.http.converter.HttpMessageConverter HttpMessageConverter} capable of
 * encoding and decoding one or more {@link PermazenObject}s contained in a {@link DetachedPermazenTransaction}.
 *
 * <p>
 * The payload MIME type is set to {@code application/x-permazen-transaction} with an additional {@code root}
 * parameter specifying the root {@link PermazenObject} in the encoded transaction.
 * There may of course be arbitrarily many other supporting {@link PermazenObject}s riding along with it.
 *
 * <p>
 * Validation of all incoming objects is supported; see {@link #setValidationGroups setValidationGroups()}.
 *
 * @see DetachedPermazenTransactionHttpMessageConverter
 * @see KVStoreHttpMessageConverter
 */
public class PermazenObjectHttpMessageConverter extends AbstractHttpMessageConverter<PermazenObject> {

    /**
     * Name of the object ID parameter incluced in the MIME type.
     */
    public static final String ROOT_OBJECT_ID_PARAMETER_NAME = "root";

    private final Permazen pdb;

    private Class<?>[] validationGroups;

    /**
     * Constructor.
     *
     * @param pdb {@link Permazen} instance
     * @throws IllegalArgumentException if {@code pdb} is null
     */
    public PermazenObjectHttpMessageConverter(Permazen pdb) {
        this(pdb, DetachedPermazenTransactionHttpMessageConverter.MIME_TYPE,
          DetachedPermazenTransactionHttpMessageConverter.LEGACY_MIME_TYPE);
    }

    /**
     * Constructor.
     *
     * @param pdb {@link Permazen} instance
     * @param supportedMediaTypes supported media types
     * @throws IllegalArgumentException if {@code pdb} is null
     */
    public PermazenObjectHttpMessageConverter(Permazen pdb, MediaType... supportedMediaTypes) {
        super(supportedMediaTypes);
        Preconditions.checkArgument(pdb != null, "null pdb");
        this.pdb = pdb;
    }

    /**
     * Set validation groups used to validate all incoming objects.
     *
     * <p>
     * If set to null, no validation is performed; this is the default behavior.
     *
     * <p>
     * Otherwise, validation of <b>all</b> incoming objects (not just the root object) is performed using the specified
     * validation groups, or {@link Default} if an empty array is specified.
     *
     * @param groups validation group(s) to use for validation; if empty, {@link Default} is assumed;
     *  if null, no validation is performed
     */
    public void setValidationGroups(Class<?>... groups) {
        this.validationGroups = groups;
    }

// AbstractHttpMessageConverter

    @Override
    protected Long getContentLength(PermazenObject jobj, MediaType contentType) {
        return KVStoreHttpMessageConverter.getKVStoreContentLength(jobj.getTransaction().getTransaction().getKVTransaction());
    }

    @Override
    public boolean canRead(Class<?> clazz, MediaType mediaType) {
        return super.canRead(mediaType);
    }

    @Override
    protected boolean supports(Class<?> target) {
        return this.pdb.findPermazenClass(target) != null;
    }

    @Override
    protected MediaType getDefaultContentType(PermazenObject jobj) {
        Preconditions.checkArgument(jobj != null, "null jobj");
        return new MediaType(this.getSupportedMediaTypes().get(0),
          Collections.<String, String>singletonMap(ROOT_OBJECT_ID_PARAMETER_NAME, jobj.getObjId().toString()));
    }

    @Override
    protected PermazenObject readInternal(Class<? extends PermazenObject> type, HttpInputMessage input) throws IOException {

        // Decode the detached transaction
        final DetachedPermazenTransaction jtx = DetachedPermazenTransactionHttpMessageConverter.readDetachedTransaction(
          this.pdb, input, this.validationGroups);

        // Get the root object's ID
        final MediaType mediaType = input.getHeaders().getContentType();
        if (mediaType == null) {
            throw new HttpMessageNotReadableException("required parameter \"" + ROOT_OBJECT_ID_PARAMETER_NAME
              + "\" missing; no `Content-Type' header found", input);
        }
        final String objId = mediaType.getParameter(ROOT_OBJECT_ID_PARAMETER_NAME);
        if (objId == null) {
            throw new HttpMessageNotReadableException("required parameter \"" + ROOT_OBJECT_ID_PARAMETER_NAME
              + "\" missing from Content-Type \"" + mediaType + "\"", input);
        }
        final ObjId id;
        try {
            id = new ObjId(objId);
        } catch (IllegalArgumentException e) {
            throw new HttpMessageNotReadableException("invalid \"" + ROOT_OBJECT_ID_PARAMETER_NAME + "\" parameter value \""
              + objId + "\" in Content-Type \"" + mediaType + "\"", input);
        }

        // Find the root object
        final PermazenObject jobj = jtx.get(id);
        if (!jobj.exists())
            throw new HttpMessageNotReadableException("no object with object ID " + id + " found in object graph", input);

        // Done
        return jobj;
    }

    @Override
    protected void writeInternal(PermazenObject jobj, HttpOutputMessage output) throws IOException {
        output.getHeaders().setContentType(this.getDefaultContentType(jobj));
        KVStoreHttpMessageConverter.writeKVStore(jobj.getTransaction().getTransaction().getKVTransaction(), output);
    }
}
