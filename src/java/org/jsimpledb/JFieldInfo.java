
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb;

import com.google.common.base.Converter;
import com.google.common.base.Preconditions;
import com.google.common.reflect.TypeToken;

import java.util.HashSet;
import java.util.List;

import org.jsimpledb.core.Transaction;

/**
 * Schema-wide information common to all {@link JField}s sharing a storage ID.
 *
 * <p>
 * As the same {@link JField} can appear in multiple object types, this class contains the information about a
 * {@link JField} that is not specific to any one {@link JClass}. This is particularly relevant for reference fields,
 * which can have different Java reference types in different {@link JClass}es.
 * </p>
 */
abstract class JFieldInfo {

    final JSimpleDB jdb;
    final int storageId;

    private final HashSet<String> names = new HashSet<>();
    private boolean requiresValidation;

    JFieldInfo(JField jfield) {
        Preconditions.checkArgument(jfield != null, "null jfield");
        this.jdb = jfield.jdb;
        this.storageId = jfield.storageId;
    }

    /**
     * Get the storage ID of this field.
     */
    public int getStorageId() {
        return this.storageId;
    }

    /**
     * Get the type of this field when the containing object type is known to be restricted to {@code context}.
     *
     * @param jdb database
     * @param context Java type containing this field
     * @throws IllegalArgumentException if no sub-type of {@code context} contains this field
     */
    public abstract TypeToken<?> getTypeToken(Class<?> context);

    /**
     * Determine whether any associated {@link JField} requires validation.
     *
     * @return true any associated fields require validation, otherwise false
     */
    public boolean isRequiresValidation() {
        return this.requiresValidation;
    }

    /**
     * Get a {@link Converter} that converts this field's value from what the core database returns
     * to what the Java application expects, or null if no conversion is needed.
     *
     * <p>
     * The implementation in {@link JFieldInfo} returns null.
     * </p>
     *
     * @param jtx transaction
     */
    public Converter<?, ?> getConverter(JTransaction jtx) {
        return null;
    }

    /**
     * Add the {@link FieldChange} sub-types that are valid parameter types for
     * @OnChange-annotated methods that watch this field as the target field.
     *
     * @param types place to add valid parameter types to
     * @param targetType the type of the class containing the changed field
     */
    abstract <T> void addChangeParameterTypes(List<TypeToken<?>> types, Class<T> targetType);

    /**
     * Register the given listener as a change listener for this field.
     */
    abstract void registerChangeListener(Transaction tx, int[] path, Iterable<Integer> types, AllChangesListener listener);

    void witness(JField jfield) {
        assert jfield.storageId == this.storageId;
        this.names.add(jfield.name);
        this.requiresValidation |= jfield.requiresValidation;
    }

// Object

    @Override
    public String toString() {
        return this.getClass().getSimpleName().replaceAll("^J(.+)FieldInfo", "$1").toLowerCase() + " field "
          + (this.names.size() == 1 ? "`" + this.names.iterator().next() + "'" : "with storage ID " + this.storageId);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null || obj.getClass() != this.getClass())
            return false;
        final JFieldInfo that = (JFieldInfo)obj;
        return this.storageId == that.storageId;
    }

    @Override
    public int hashCode() {
        return this.storageId;
    }
}

