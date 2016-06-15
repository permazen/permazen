
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb;

import java.lang.reflect.Method;
import java.util.List;

import org.jsimpledb.annotation.JField;
import org.jsimpledb.annotation.JListField;
import org.jsimpledb.annotation.JSimpleClass;

/**
 * Scans for {@link JListField &#64;JListField} annotations.
 */
class JListFieldScanner<T> extends AbstractFieldScanner<T, JListField> {

    JListFieldScanner(JClass<T> jclass, JSimpleClass jsimpleClass) {
        super(jclass, JListField.class, jsimpleClass);
    }

    @Override
    protected JListField getDefaultAnnotation() {
        return new JListField() {
            @Override
            public Class<JListField> annotationType() {
                return JListField.class;
            }
            @Override
            public String name() {
                return "";
            }
            @Override
            public int storageId() {
                return 0;
            }
            @Override
            public JField element() {
                return JFieldScanner.DEFAULT_JFIELD;
            }
        };
    }

    @Override
    protected boolean includeMethod(Method method, JListField annotation) {
        this.checkNotStatic(method);
        this.checkReturnType(method, List.class);
        this.checkParameterTypes(method);
        return true;
    }

    @Override
    protected boolean isAutoPropertyCandidate(Method method) {
        return super.isAutoPropertyCandidate(method) && List.class.isAssignableFrom(method.getReturnType());
    }
}

