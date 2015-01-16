
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb;

import com.google.common.reflect.TypeToken;

import java.util.List;

import org.jsimpledb.core.Transaction;

class JCounterFieldInfo extends JFieldInfo {

    JCounterFieldInfo(JCounterField jfield) {
        super(jfield);
    }

    @Override
    public TypeToken<?> getTypeToken(TypeToken<?> context) {
        return TypeToken.of(Counter.class);
    }

    @Override
    <T> void addChangeParameterTypes(List<TypeToken<?>> types, TypeToken<T> targetType) {
        throw new UnsupportedOperationException("counter fields do not support change notifications");
    }

    @Override
    void registerChangeListener(Transaction tx, int[] path, Iterable<Integer> types, AllChangesListener listener) {
        throw new UnsupportedOperationException("counter fields do not support change notifications");
    }
}

