
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.tuple;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

class AbstractHas4<V1, V2, V3, V4> extends AbstractHas3<V1, V2, V3> implements Has4<V1, V2, V3, V4> {

    final V4 v4;

    protected AbstractHas4(V1 v1, V2 v2, V3 v3, V4 v4) {
        super(v1, v2, v3);
        this.v4 = v4;
    }

    @Override
    public V4 getValue4() {
        return this.v4;
    }

    @Override
    public int getSize() {
        return 4;
    }

    @Override
    public List<Object> asList() {
        return Collections.unmodifiableList(Arrays.asList(this.v1, this.v2, this.v3, this.v4));
    }

    @Override
    void addValues(StringBuilder buf) {
        super.addValues(buf);
        buf.append(", ");
        buf.append(this.v4);
    }

    @Override
    boolean compareValues(Object obj) {
        final AbstractHas4<?, ?, ?, ?> that = (AbstractHas4<?, ?, ?, ?>)obj;
        return super.compareValues(that) && Objects.equals(this.v4, that.v4);
    }
}
