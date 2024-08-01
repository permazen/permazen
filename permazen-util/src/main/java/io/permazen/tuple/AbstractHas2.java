
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.tuple;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

class AbstractHas2<V1, V2> extends AbstractHas1<V1> implements Has2<V1, V2> {

    final V2 v2;

    protected AbstractHas2(V1 v1, V2 v2) {
        super(v1);
        this.v2 = v2;
    }

    @Override
    public V2 getValue2() {
        return this.v2;
    }

    @Override
    public int getSize() {
        return 2;
    }

    @Override
    public List<Object> asList() {
        return Collections.unmodifiableList(Arrays.asList(this.v1, this.v2));
    }

    @Override
    void addValues(StringBuilder buf) {
        super.addValues(buf);
        buf.append(", ");
        buf.append(this.v2);
    }

    @Override
    boolean compareValues(Object obj) {
        final AbstractHas2<?, ?> that = (AbstractHas2<?, ?>)obj;
        return super.compareValues(that) && Objects.equals(this.v2, that.v2);
    }
}
