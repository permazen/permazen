
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb;

import java.util.Arrays;

class IndexQueryInfoKey {

    private final Class<?>[] types;
    private final boolean composite;
    private final String name;

    IndexQueryInfoKey(String name, boolean composite, Class<?>... types) {
        this.name = name;
        this.composite = composite;
        this.types = types;
    }

    public IndexQueryInfo getIndexQueryInfo(JSimpleDB jdb) {

        // Handle composite index
        if (this.composite)
            return new IndexQueryInfo(jdb, this.types[0], this.name, Arrays.copyOfRange(this.types, 1, this.types.length));

        // Handle map value index
        if (this.types.length == 3)
            return new IndexQueryInfo(jdb, this.types[0], this.name, this.types[1], this.types[2]);

        // Handle all others
        return new IndexQueryInfo(jdb, this.types[0], this.name, this.types[1]);
    }

// Object

    @Override
    public boolean equals(Object obj) {
        if (obj == this)
            return true;
        if (obj == null || obj.getClass() != this.getClass())
            return false;
        final IndexQueryInfoKey that = (IndexQueryInfoKey)obj;
        return this.name.equals(that.name)
          && this.composite == that.composite
          && Arrays.equals(this.types, that.types);
    }

    @Override
    public int hashCode() {
        return this.name.hashCode()
          ^ (this.composite ? ~0 : 0)
          ^ Arrays.hashCode(this.types);
    }
}

