
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb;

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.jsimpledb.annotation.JField;
import org.jsimpledb.annotation.JSimpleClass;
import org.jsimpledb.core.DeleteAction;

/**
 * Scans for {@link JField &#64;JField} annotations.
 */
class JFieldScanner<T> extends AbstractFieldScanner<T, JField> {

    JFieldScanner(JClass<T> jclass, JSimpleClass jsimpleClass) {
        super(jclass, JField.class, jsimpleClass);
    }

    @Override
    protected JField getDefaultAnnotation() {
        return JFieldScanner.getDefaultJField(this.jsimpleClass);
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
            final Method setter;
            try {
                setter = Util.findJFieldSetterMethod(this.jclass.type, method);
            } catch (IllegalArgumentException e) {
                return false;
            }
            if (!this.jsimpleClass.autogenNonAbstract() && (setter.getModifiers() & Modifier.ABSTRACT) == 0)
                return false;
        }
        return true;
    }

    public static final JField getDefaultJField(final JSimpleClass jsimpleClass) {
        return new DefaultJField(jsimpleClass);
    }

// DefaultJField

    public static class DefaultJField implements JField {

        private JSimpleClass jsimpleClass;

        DefaultJField(JSimpleClass jsimpleClass) {
            this.jsimpleClass = jsimpleClass;
        }

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
        public long typeSignature() {
            return 0;
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
        @Override
        public boolean allowDeleted() {
            return this.jsimpleClass.autogenAllowDeleted();
        }
        @Override
        public boolean allowDeletedSnapshot() {
            return this.jsimpleClass.autogenAllowDeletedSnapshot();
        }
        @Override
        public UpgradeConversionPolicy upgradeConversion() {
            return this.jsimpleClass.autogenUpgradeConversion();
        }
    };
}
