
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen;

import java.lang.reflect.Method;
import java.util.Arrays;

/**
 * Object identifying a method name, parameter types, and return type.
 */
class MethodKey {

    private final String name;
    private final Class<?>[] parameterTypes;
    private final Class<?> returnType;

    MethodKey(Method method) {
        this.name = method.getName();
        this.returnType = method.getReturnType();
        this.parameterTypes = method.getParameterTypes();
    }

    @Override
    public int hashCode() {
        int hash = this.name.hashCode();
        for (Class<?> parameterType : this.parameterTypes)
            hash = (hash * 31) + parameterType.hashCode();
        hash ^= this.returnType.hashCode();
        return hash;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this)
            return true;
        if (obj == null || obj.getClass() != this.getClass())
            return false;
        final MethodKey that = (MethodKey)obj;
        return this.name.equals(that.name)
          && this.returnType == that.returnType
          && Arrays.equals(this.parameterTypes, that.parameterTypes);
    }
}

