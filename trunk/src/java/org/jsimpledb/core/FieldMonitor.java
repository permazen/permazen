
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.core;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;

import org.jsimpledb.kv.KeyRange;
import org.jsimpledb.kv.KeyRanges;
import org.jsimpledb.kv.SimpleKeyRanges;

/**
 * A monitor for changes within a {@link Transaction} of the value of a specific field, as seen through a path of references.
 */
class FieldMonitor {

    /**
     * Sorts instances by length of reference path.
     */
    public static final Comparator<FieldMonitor> SORT_BY_PATH_LENGTH = new Comparator<FieldMonitor>() {
        @Override
        public int compare(FieldMonitor monitor1, FieldMonitor monitor2) {
            return Integer.compare(monitor1.path.length, monitor2.path.length);
        }
    };

    final int storageId;
    final int[] path;
    final KeyRanges types;
    final Object listener;

    /**
     * Constructor.
     *
     * @param storageId storage ID of the field to monitor
     * @param path path of references to {@code field}
     * @param types set of allowed storage IDs for the changed object, or null for no restriction
     * @param listener listener to notify
     */
    FieldMonitor(int fieldStorageId, int[] path, Iterable<Integer> types, Object listener) {
        if (path == null)
            throw new IllegalArgumentException("null path");
        if (listener == null)
            throw new IllegalArgumentException("null listener");
        this.storageId = fieldStorageId;
        if (types != null) {
            final ArrayList<KeyRange> keyRanges = new ArrayList<>();
            for (int objTypeStorageId : types)
                keyRanges.add(ObjId.getKeyRange(objTypeStorageId));
            this.types = new SimpleKeyRanges(keyRanges);
        } else
            this.types = null;
        this.path = path.clone();
        this.listener = listener;
    }

// Object

    @Override
    public int hashCode() {
        return this.storageId
          ^ Arrays.hashCode(this.path)
          ^ (this.types != null ? this.types.hashCode() : 0)
          ^ this.listener.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this)
            return true;
        if (obj == null || obj.getClass() != this.getClass())
            return false;
        final FieldMonitor that = (FieldMonitor)obj;
        return this.storageId == that.storageId
          && Arrays.equals(this.path, that.path)
          && (this.types != null ? this.types.equals(that.types) : that.types == null)
          && this.listener.equals(that.listener);
    }
}

