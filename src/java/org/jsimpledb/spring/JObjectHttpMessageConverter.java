
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb.spring;

import com.google.common.base.Preconditions;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

import org.jsimpledb.JClass;
import org.jsimpledb.JObject;
import org.jsimpledb.JSimpleDB;
import org.jsimpledb.SnapshotJTransaction;
import org.jsimpledb.core.UnknownTypeException;
import org.jsimpledb.schema.SchemaObjectType;
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
 * The payload MIME type is set to {@code application/x-jsimpledb-FOO} where {@code FOO} is the
 * {@linkplain org.jsimpledb.JClass#getName name} of the JSimpleDB object type.
 * There must be exactly one instance of this object type in the encoded {@link SnapshotJTransaction},
 * but there may be arbitrarily many other "supporting" {@link JObject}s riding along with it.
 *
 * <p>
 * Validation of all incoming objects is supported; see {@link #setValidationGroups setValidationGroups()}.
 *
 * @see SnapshotJTransactionHttpMessageConverter
 * @see KVStoreHttpMessageConverter
 */
public class JObjectHttpMessageConverter extends AbstractHttpMessageConverter<JObject> {

    private static final String MEDIA_SUBTYPE_PREFIX = "x-jsimpledb-";

    private final JSimpleDB jdb;

    private final ConcurrentHashMap<MediaType, JClass<?>> mediaTypeMap = new ConcurrentHashMap<>();
    private Class<?>[] validationGroups;

    /**
     * Constructor.
     *
     * @param mediaTypes custom {@link MediaType}s supported by overriding subclasses
     * @param jdb {@link JSimpleDB} instance
     * @throws IllegalArgumentException if {@code jdb} is null
     */
    public JObjectHttpMessageConverter(JSimpleDB jdb, MediaType... mediaTypes) {
        super(mediaTypes);
        Preconditions.checkArgument(jdb != null, "null jdb");
        this.jdb = jdb;
    }

    /**
     * Constructor.
     *
     * <p>
     * Configures this instance for the MIME types {@code application/x-jsimpledb-foo}, {@code application/x-jsimpledb-bar}, etc.
     * where {@code foo}, {@code bar}, etc. are {@code jdb}'s object type names.
     *
     * @param jdb {@link JSimpleDB} instance
     * @throws IllegalArgumentException if {@code jdb} is null
     */
    public JObjectHttpMessageConverter(JSimpleDB jdb) {
        this(jdb, JObjectHttpMessageConverter.generateMediaTypes(jdb));
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
    public boolean canRead(Class<?> target, MediaType mediaType) {
        if (!this.canRead(mediaType))
            return false;
        final JClass<?> jclass = this.findJClass(mediaType);
        if (jclass == null)
            return false;
        return target.isAssignableFrom(jclass.getType());
    }

    @Override
    public boolean canWrite(Class<?> target, MediaType mediaType) {
        if (!this.canWrite(mediaType))
            return false;
        final JClass<?> jclass = this.findJClass(mediaType);
        if (jclass == null)
            return false;
        return jclass.getType().isAssignableFrom(target);
    }

    @Override
    protected boolean supports(Class<?> target) {
        return this.jdb.getJClassesByType().containsKey(target);
    }

    @Override
    protected MediaType getDefaultContentType(JObject jobj) {
        Preconditions.checkArgument(jobj != null, "null jobj");
        return JObjectHttpMessageConverter.createMediaType(this.jdb.getJClass(jobj.getObjId()));
    }

    @Override
    protected JObject readInternal(Class<? extends JObject> type, HttpInputMessage input) throws IOException {

        // Get snapshot transaction
        final SnapshotJTransaction jtx = SnapshotJTransactionHttpMessageConverter.readSnapshotTransaction(
          this.jdb, input, this.validationGroups);

        // Get root object and verify there is exactly one of them
        final Iterator<? extends JObject> i = jtx.getAll(type).iterator();
        if (!i.hasNext())
            throw new HttpMessageNotReadableException("no object of type " + type.getName() + " found in object graph");
        final JObject jobj = i.next();
        if (i.hasNext())
            throw new HttpMessageNotReadableException("more than one object of type " + type.getName() + " found in object graph");

        // Done
        return jobj;
    }

    @Override
    protected void writeInternal(JObject jobj, HttpOutputMessage output) throws IOException {
        KVStoreHttpMessageConverter.writeKVStore(jobj.getTransaction().getTransaction().getKVTransaction(), output);
    }

// Internal methods

    private JClass<?> findJClass(MediaType mediaType) {

        // Sanity check
        Preconditions.checkArgument(mediaType != null, "null mediaType");

        // Check cache
        final JClass<?> cached = this.mediaTypeMap.get(mediaType);
        if (cached != null)
            return cached;

        // Decode MIME type into JClass
        final String subtype = mediaType.getSubtype();
        final int prefixLength = MEDIA_SUBTYPE_PREFIX.length();
        if (subtype == null || subtype.length() < prefixLength || !subtype.substring(0, prefixLength).equals(MEDIA_SUBTYPE_PREFIX))
            return null;
        final String typeName = subtype.substring(prefixLength);
        final SchemaObjectType objectType = this.jdb.getNameIndex().getSchemaObjectType(typeName);
        if (objectType == null)
            return null;
        final JClass<?> jclass;
        try {
            jclass = this.jdb.getJClass(objectType.getStorageId());
        } catch (UnknownTypeException e) {
            return null;
        }

        // Cache and return result
        this.mediaTypeMap.put(mediaType, jclass);
        return jclass;
    }

    private static MediaType[] generateMediaTypes(JSimpleDB jdb) {
        Preconditions.checkArgument(jdb != null, "null jdb");
        final List<JClass<? extends Object>> jclasses = jdb.getJClasses(Object.class);
        final ArrayList<MediaType> mediaTypes = new ArrayList<>(jclasses.size());
        for (JClass<? extends Object> jclass : jclasses)
            mediaTypes.add(JObjectHttpMessageConverter.createMediaType(jclass));
        return mediaTypes.toArray(new MediaType[mediaTypes.size()]);
    }

    private static MediaType createMediaType(JClass<?> jclass) {
        return new MediaType("application", MEDIA_SUBTYPE_PREFIX + jclass.getName());
    }
}

