
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.tuple;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

class AbstractHas1<V1> implements Tuple, Has1<V1> {

    final V1 v1;

    protected AbstractHas1(V1 v1) {
        this.v1 = v1;
    }

    @Override
    public V1 getValue1() {
        return this.v1;
    }

    @Override
    public int getSize() {
        return 1;
    }

    @Override
    public List<Object> asList() {
        return Collections.unmodifiableList(Arrays.<Object>asList(this.v1));
    }

// Object

    @Override
    public String toString() {
        final StringBuilder buf = new StringBuilder();
        buf.append('<');
        this.addValues(buf);
        buf.append('>');
        return buf.toString();
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this)
            return true;
        if (obj == null || obj.getClass() != this.getClass())
            return false;
        return this.compareValues(obj);
    }

    @Override
    public int hashCode() {
        return this.getClass().hashCode() ^ this.asList().hashCode();
    }

    /**
     * Add values in string form to the buffer. Used to implement {@link #toString}.
     * Each value should be preceded by the string {@code ", "}.
     * Subclasses must first invoke {@code super.addValues()}.
     */
    void addValues(StringBuilder buf) {
        buf.append(this.v1);
    }

    /**
     * Compare values for equality. Used to implement {@link #equals equals()}.
     * Subclasses must remember to include an invocation of {@code super.compareValues()}.
     *
     * @param obj other object which is guaranteed to have the same Java type as this instance
     */
    boolean compareValues(Object obj) {
        final AbstractHas1<?> that = (AbstractHas1<?>)obj;
        return Objects.equals(this.v1, that.v1);
    }
}
