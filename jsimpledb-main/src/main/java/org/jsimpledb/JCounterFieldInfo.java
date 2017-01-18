
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb;

import com.google.common.reflect.TypeToken;

import java.util.Collections;
import java.util.List;
import java.util.Set;

class JCounterFieldInfo extends JFieldInfo {

    JCounterFieldInfo(JCounterField jfield) {
        super(jfield);
    }

    @Override
    public Set<TypeToken<?>> getTypeTokens(Class<?> context) {
        return Collections.<TypeToken<?>>singleton(TypeToken.of(Counter.class));
    }

    @Override
    <T> void addChangeParameterTypes(List<TypeToken<?>> types, Class<T> targetType) {
        throw new UnsupportedOperationException("counter fields do not support change notifications");
    }
}

