
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb.parse.expr;

import java.lang.reflect.Constructor;
import java.lang.reflect.Type;

class ConstructorExecutable extends Executable<Constructor<?>> {

    ConstructorExecutable(Constructor<?> constructor) {
        super(constructor);
    }

    @Override
    public Class<?> getReturnType() {
        return void.class;
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
