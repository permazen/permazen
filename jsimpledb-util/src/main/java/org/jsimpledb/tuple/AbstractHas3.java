
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb.tuple;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

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
        return super.hashCode() ^ Objects.hashCode(this.v3);
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
        return super.compareValues(that) && Objects.equals(this.v3, that.v3);
    }
}

