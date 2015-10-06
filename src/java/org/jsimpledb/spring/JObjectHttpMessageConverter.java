
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb.spring;

import com.google.common.base.Preconditions;

import java.io.IOException;
import java.util.Collections;

import org.jsimpledb.JObject;
import org.jsimpledb.JSimpleDB;
import org.jsimpledb.SnapshotJTransaction;
import org.jsimpledb.core.ObjId;
import org.springframework.http.HttpInputMessage;
import org.springframework.http.HttpOutputMessage;
import org.springframework.http.MediaType;
import org.springframework.http.converter.AbstractHttpMessageConverter;
import org.springframework.http.converter.HttpMessageNotReadableException;

/**
 * Spring {@link org.springframework.http.converter.HttpMessageConverter HttpMessageConverter} capable of
 * encoding and decoding one or more {@link JObject}s contained in a {@link SnapshotJTransaction} that is
 * backed by a {@link org.jsimpledb.kv.util.NavigableMapKVStore}.
 *
 * <p>
 * The payload MIME type is set to {@code application/x-jsimpledb-transaction} with an additional {@code root}
 * parameter specifying the root {@link JObject} in the encoded transaction.
 * There may of course be arbitrarily many other supporting {@link JObject}s riding along with it.
 *
 * <p>
 * Validation of all incoming objects is supported; see {@link #setValidationGroups setValidationGroups()}.
 *
 * @see SnapshotJTransactionHttpMessageConverter
 * @see KVStoreHttpMessageConverter
 */
public class JObjectHttpMessageConverter extends AbstractHttpMessageConverter<JObject> {

    /**
     * Name of the object ID parameter incluced in the MIME type.
     */
    public static final String ROOT_OBJECT_ID_PARAMETER_NAME = "root";

    private final JSimpleDB jdb;

    private Class<?>[] validationGroups;

    /**
     * Constructor.
     *
     * @param jdb {@link JSimpleDB} instance
     * @throws IllegalArgumentException if {@code jdb} is null
     */
    public JObjectHttpMessageConverter(JSimpleDB jdb) {
        super(SnapshotJTransactionHttpMessageConverter.MIME_TYPE);
        Preconditions.checkArgument(jdb != null, "null jdb");
        this.jdb = jdb;
    }

    /**
     * Set validation groups used to validate all incoming objects.
     *
     * <p>
     * If set to null, no validation is performed. Otherwise, validation of <b>all</b> incoming objects (not just
     * the root) is performed using the specified validation groups, or {@link javax.validation.groups.Default} if
     * an empty is specified.
     *
     * <p>
     * By default, this is null.
     *
     * @param groups validation group(s) to use for validation; if empty, {@link javax.validation.groups.Default} is assumed;
     *  if null, no validation is performed
     */
    public void setValidationGroups(Class<?>... groups) {
        this.validationGroups = groups;
    }

// AbstractHttpMessageConverter

    @Override
    protected Long getContentLength(JObject jobj, MediaType contentType) {
        return KVStoreHttpMessageConverter.getKVStoreContentLength(jobj.getTransaction().getTransaction().getKVTransaction());
    }

    @Override
    protected boolean supports(Class<?> target) {
        return this.jdb.findJClass(target) != null;
    }

    @Override
    protected MediaType getDefaultContentType(JObject jobj) {
        Preconditions.checkArgument(jobj != null, "null jobj");
        return new MediaType(SnapshotJTransactionHttpMessageConverter.MIME_TYPE,
          Collections.<String, String>singletonMap(ROOT_OBJECT_ID_PARAMETER_NAME, jobj.getObjId().toString()));
    }

    @Override
    protected JObject readInternal(Class<? extends JObject> type, HttpInputMessage input) throws IOException {

        // Decode the snapshot transaction
        final SnapshotJTransaction jtx = SnapshotJTransactionHttpMessageConverter.readSnapshotTransaction(
          this.jdb, input, this.validationGroups);

        // Get the root object's ID
        final MediaType mediaType = input.getHeaders().getContentType();
        if (!SnapshotJTransactionHttpMessageConverter.MIME_TYPE.includes(mediaType))
            throw new HttpMessageNotReadableException("invalid Content-Type `" + mediaType + "'");
        final String objId = mediaType.getParameter(ROOT_OBJECT_ID_PARAMETER_NAME);
        if (objId == null) {
            throw new HttpMessageNotReadableException("required parameter `" + ROOT_OBJECT_ID_PARAMETER_NAME
              + "' missing from Content-Type `" + mediaType + "'");
        }
        final ObjId id;
        try {
            id = new ObjId(objId);
        } catch (IllegalArgumentException e) {
            throw new HttpMessageNotReadableException("invalid `" + ROOT_OBJECT_ID_PARAMETER_NAME + "' parameter value `"
              + objId + "' in Content-Type `" + mediaType + "'");
        }

        // Find the root object
        final JObject jobj = jtx.getJObject(id);
        if (!jobj.exists())
            throw new HttpMessageNotReadableException("no object with object ID " + id + " found in object graph");

        // Done
        return jobj;
    }

    @Override
    protected void writeInternal(JObject jobj, HttpOutputMessage output) throws IOException {
        KVStoreHttpMessageConverter.writeKVStore(jobj.getTransaction().getTransaction().getKVTransaction(), output);
    }
}

