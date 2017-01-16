
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb.tuple;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

class AbstractHas5<V1, V2, V3, V4, V5> extends AbstractHas4<V1, V2, V3, V4> implements Has5<V1, V2, V3, V4, V5> {

    final V5 v5;

    protected AbstractHas5(V1 v1, V2 v2, V3 v3, V4 v4, V5 v5) {
        super(v1, v2, v3, v4);
        this.v5 = v5;
    }

    @Override
    public V5 getValue5() {
        return this.v5;
    }

    @Override
    public int getSize() {
        return 5;
    }

    @Override
    public List<Object> asList() {
        return Collections.unmodifiableList(Arrays.asList(this.v1, this.v2, this.v3, this.v4, this.v5));
    }

    @Override
    public int hashCode() {
        return super.hashCode() ^ Objects.hashCode(this.v5);
    }

    @Override
    void addValues(StringBuilder buf) {
        super.addValues(buf);
        buf.append(", ");
        buf.append(this.v5);
    }

    @Override
    boolean compareValues(Object obj) {
        final AbstractHas5<?, ?, ?, ?, ?> that = (AbstractHas5<?, ?, ?, ?, ?>)obj;
        return super.compareValues(that) && Objects.equals(this.v5, that.v5);
    }
}

