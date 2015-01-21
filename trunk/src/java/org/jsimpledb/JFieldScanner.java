
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb;

import java.lang.reflect.Method;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.jsimpledb.annotation.JField;
import org.jsimpledb.core.DeleteAction;

/**
 * Scans for {@link JField &#64;JField} annotations.
 */
class JFieldScanner<T> extends AbstractFieldScanner<T, JField> {

    public static final JField DEFAULT_JFIELD = new JField() {
        @Override
        public Class<JField> annotationType() {
            return JField.class;
        }
        @Override
        public String name() {
            return "";
        }
        @Override
        public String type() {
            return "";
        }
        @Override
        public int storageId() {
            return 0;
        }
        @Override
        public boolean indexed() {
            return false;
        }
        @Override
        public boolean unique() {
            return false;
        }
        @Override
        public String[] uniqueExclude() {
            return new String[0];
        }
        @Override
        public boolean uniqueExcludeNull() {
            return false;
        }
        @Override
        public DeleteAction onDelete() {
            return DeleteAction.EXCEPTION;
        }
        @Override
        public boolean cascadeDelete() {
            return false;
        }
    };

    JFieldScanner(JClass<T> jclass, boolean autogenFields) {
        super(jclass, JField.class, autogenFields);
    }

    @Override
    protected JField getDefaultAnnotation() {
        return DEFAULT_JFIELD;
    }

    @Override
    protected boolean includeMethod(Method method, JField annotation) {
        this.checkNotStatic(method);
        this.checkParameterTypes(method);
        if (method.getReturnType().equals(Void.TYPE))
            throw new IllegalArgumentException(this.getErrorPrefix(method) + "method returns void");
        return true;
    }

    @Override
    protected boolean isAutoPropertyCandidate(Method method) {
        if (!super.isAutoPropertyCandidate(method))
            return false;
        final Class<?> returnType = method.getReturnType();
        if (List.class.isAssignableFrom(returnType)
          || Set.class.isAssignableFrom(returnType)
          || Map.class.isAssignableFrom(returnType))
            return false;
        if (returnType != Counter.class) {
            try {
                Util.findSetterMethod(this.jclass.type, method);
            } catch (IllegalArgumentException e) {
                return false;
            }
        }
        return true;
    }
}

