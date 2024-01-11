
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen;

import io.permazen.annotation.PermazenField;
import io.permazen.annotation.PermazenType;
import io.permazen.core.DeleteAction;

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Scans for {@link PermazenField &#64;PermazenField} annotations.
 */
class PermazenFieldScanner<T> extends AbstractPermazenFieldScanner<T, PermazenField> {

    PermazenFieldScanner(PermazenClass<T> pclass, PermazenType permazenType) {
        super(pclass, PermazenField.class, permazenType);
    }

    @Override
    protected PermazenField getDefaultAnnotation() {
        return PermazenFieldScanner.getDefaultPermazenField(this.permazenType);
    }

    @Override
    protected boolean includeMethod(Method method, PermazenField annotation) {
        this.checkNotStatic(method);
        this.checkParameterTypes(method);
        if (method.getReturnType().equals(Void.TYPE))
            throw new IllegalArgumentException(String.format("%s: method returns void", this.getErrorPrefix(method)));
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
                setter = Util.findPermazenFieldSetterMethod(this.pclass.type, method);
            } catch (IllegalArgumentException e) {
                return false;
            }
            if (!this.permazenType.autogenNonAbstract() && (setter.getModifiers() & Modifier.ABSTRACT) == 0)
                return false;
        }
        return true;
    }

    public static final PermazenField getDefaultPermazenField(final PermazenType permazenType) {
        return new DefaultPermazenField(permazenType);
    }

// DefaultPermazenField

    public static class DefaultPermazenField implements PermazenField {

        private PermazenType permazenType;

        DefaultPermazenField(PermazenType permazenType) {
            this.permazenType = permazenType;
        }

        @Override
        public Class<PermazenField> annotationType() {
            return PermazenField.class;
        }
        @Override
        public String name() {
            return "";
        }
        @Override
        public String encoding() {
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
        public String[] forwardCascades() {
            return new String[0];
        }
        @Override
        public String[] inverseCascades() {
            return new String[0];
        }
        @Override
        public DeleteAction inverseDelete() {
            return DeleteAction.EXCEPTION;
        }
        @Override
        public boolean forwardDelete() {
            return false;
        }
        @Override
        public boolean allowDeleted() {
            return this.permazenType.autogenAllowDeleted();
        }
        @Override
        public UpgradeConversionPolicy upgradeConversion() {
            return this.permazenType.autogenUpgradeConversion();
        }
    };
}
