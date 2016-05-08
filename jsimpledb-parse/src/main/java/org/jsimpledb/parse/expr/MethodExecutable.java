
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb.parse.expr;

import java.lang.reflect.Method;
import java.lang.reflect.Type;

class MethodExecutable extends Executable<Method> {

    MethodExecutable(Method method) {
        super(method);
    }

    @Override
    public Class<?> getReturnType() {
        return this.member.getReturnType();
    }

    @Override
    public Class<?>[] getParameterTypes() {
        return this.member.getParameterTypes();
    }

    @Override
    public Type[] getGenericParameterTypes() {
        return this.member.getGenericParameterTypes();
    }

    @Override
    public boolean isVarArgs() {
        return this.member.isVarArgs();
    }
}
