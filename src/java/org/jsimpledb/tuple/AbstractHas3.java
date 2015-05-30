
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb.tuple;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

class AbstractHas3<V1, V2, V3> extends AbstractHas2<V1, V2> implements Has3<V1, V2, V3> {

    final V3 v3;

    protected AbstractHas3(V1 v1, V2 v2, V3 v3) {
        super(v1, v2);
        this.v3 = v3;
    }

    @Override
    public V3 getValue3() {
        return this.v3;
    }

    @Override
    public int getSize() {
        return 3;
    }

    @Override
    public List<Object> asList() {
        return Collections.unmodifiableList(Arrays.asList(this.v1, this.v2, this.v3));
    }

    @Override
    public int hashCode() {
        return super.hashCode() ^ (this.v3 != null ? this.v3.hashCode() : 0);
    }

    @Override
    void addValues(StringBuilder buf) {
        super.addValues(buf);
        buf.append(", ");
        buf.append(this.v3);
    }

    @Override
    boolean compareValues(Object obj) {
        final AbstractHas3<?, ?, ?> that = (AbstractHas3<?, ?, ?>)obj;
        return super.compareValues(that) && (this.v3 != null ? this.v3.equals(that.v3) : that.v3 == null);
    }
}

