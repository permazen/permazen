
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.core;

import io.permazen.encoding.Encoding;

import java.util.NavigableSet;
import java.util.Set;
import java.util.function.Predicate;

/**
 * Represents an index on a simple field, either a regular simple field or a sub-field of a complex field.
 */
abstract class SimpleFieldStorageInfo<T> extends IndexStorageInfo {

    final Encoding<T> encoding;

    SimpleFieldStorageInfo(SimpleField<T> field) {
        super(field.storageId);
        this.encoding = field.encoding.genericizeForIndex();
    }

    CoreIndex<T, ObjId> getIndex(Transaction tx) {
        return new CoreIndex<>(tx.kvt, new IndexView<>(this.storageId, this.encoding, Encodings.OBJ_ID));
    }

    /**
     * Remove all references from objects in the specified referrers set to the specified target through
     * the reference field associated with this instance. Used to implement {@link DeleteAction#UNREFERENCE}.
     *
     * <p>
     * This method may assume that this instance's {@link Encoding} is reference.
     *
     * @param tx transaction
     * @param target referenced object being deleted
     * @param referrers objects that refer to {@code target} via this reference field
     */
    abstract void unreferenceAll(Transaction tx, ObjId target, NavigableSet<ObjId> referrers);

    /**
     * Read this field from the given object and add non-null value(s) to the given set.
     *
     * @param tx transaction
     * @param id object being accessed
     * @param values read values
     * @param filter optional filter to apply
     */
    abstract void readAllNonNull(Transaction tx, ObjId id, Set<T> values, Predicate<? super T> filter);

// Object

    @Override
    public boolean equals(Object obj) {
        if (obj == this)
            return true;
        if (!super.equals(obj))
            return false;
        final SimpleFieldStorageInfo<?> that = (SimpleFieldStorageInfo<?>)obj;
        return this.encoding.equals(that.encoding);
    }

    @Override
    public int hashCode() {
        return super.hashCode() ^ this.encoding.hashCode();
    }
}
