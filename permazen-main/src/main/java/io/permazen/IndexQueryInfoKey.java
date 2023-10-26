
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen;

import java.util.Arrays;

class IndexQueryInfoKey {

    private final String name;
    private final boolean composite;
    private final Class<?> targetType;
    private final Class<?>[] valueTypes;

    IndexQueryInfoKey(String name, boolean composite, Class<?> targetType, Class<?>... valueTypes) {
        assert name != null;
        assert targetType != null;
        assert valueTypes != null;
        assert valueTypes.length > 0;
        this.name = name;
        this.composite = composite;
        this.targetType = targetType;
        this.valueTypes = valueTypes;
    }

    public IndexQueryInfo getIndexQueryInfo(Permazen jdb) {

        // Handle composite index
        if (this.composite)
            return new IndexQueryInfo(jdb, this.targetType, this.name, this.valueTypes);

        // Handle map value index
        if (this.valueTypes.length == 2)
            return new IndexQueryInfo(jdb, this.targetType, this.name, this.valueTypes[0], this.valueTypes[1]);

        // Handle all others
        assert this.valueTypes.length == 1;
        return new IndexQueryInfo(jdb, this.targetType, this.name, this.valueTypes[0]);
    }

// Object

    @Override
    public String toString() {
        return this.getClass().getSimpleName()
          + "[name=" + this.name
          + ",composite=" + this.composite
          + ",targetType=" + this.targetType
          + ",valueTypes=" + Arrays.asList(this.valueTypes)
          + "]";
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this)
            return true;
        if (obj == null || obj.getClass() != this.getClass())
            return false;
        final IndexQueryInfoKey that = (IndexQueryInfoKey)obj;
        return this.name.equals(that.name)
          && this.composite == that.composite
          && this.targetType.equals(that.targetType)
          && Arrays.equals(this.valueTypes, that.valueTypes);
    }

    @Override
    public int hashCode() {
        return this.name.hashCode()
          ^ (this.composite ? ~0 : 0)
          ^ this.targetType.hashCode()
          ^ Arrays.hashCode(this.valueTypes);
    }
}
