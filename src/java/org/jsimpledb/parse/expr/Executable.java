
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb.parse.expr;

import java.lang.reflect.Member;
import java.lang.reflect.Type;

/**
 * Substitute for {@code java.lang.reflect.Executable}, which is only available on Java 8.
 */
abstract class Executable<T extends Member> {

    final T member;

    Executable(T member) {
        this.member = member;
    }

    public T getMember() {
        return this.member;
    }

    public String getName() {
        return this.member.getName();
    }

    @Override
    public String toString() {
        return this.member.toString();
    }

    public abstract Class<?> getReturnType();
    public abstract Class<?>[] getParameterTypes();
    public abstract Type[] getGenericParameterTypes();
    public abstract boolean isVarArgs();
}

