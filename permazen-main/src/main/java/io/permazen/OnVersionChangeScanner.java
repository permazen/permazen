
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen;

import com.google.common.reflect.TypeToken;

import io.permazen.annotation.OnVersionChange;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

/**
 * Scans for {@link OnVersionChange &#64;OnVersionChange} annotations.
 */
class OnVersionChangeScanner<T> extends AnnotationScanner<T, OnVersionChange>
  implements Comparator<OnVersionChangeScanner<T>.MethodInfo> {

    private final TypeToken<Map<Integer, Object>> byStorageIdType;
    private final TypeToken<Map<String, Object>> byNameType;

    @SuppressWarnings("serial")
    OnVersionChangeScanner(JClass<T> jclass) {
        super(jclass, OnVersionChange.class);
        this.byStorageIdType = new TypeToken<Map<Integer, Object>>() { };
        this.byNameType = new TypeToken<Map<String, Object>>() { };
    }

    @Override
    protected boolean includeMethod(Method method, OnVersionChange annotation) {

        // Sanity check annotation
        if (annotation.oldVersion() < 0) {
            throw new IllegalArgumentException(String.format(
              "@%s has illegal negative oldVersion", this.annotationType.getSimpleName()));
        }
        if (annotation.newVersion() < 0) {
            throw new IllegalArgumentException(String.format(
              "@%s has illegal negative newVersion", this.annotationType.getSimpleName()));
        }

        // Check method types
        this.checkNotStatic(method);
        this.checkReturnType(method, void.class);
        final int numParams = method.getParameterTypes().length;

        // Handle @OnVersionChange version numbers; as special case, allow both to be completely omitted
        int index = 0;
        if (!(annotation.oldVersion() == 0 && annotation.newVersion() == 0 && numParams == 1)) {
            if (annotation.oldVersion() == 0)
                this.checkParameterType(method, index++, TypeToken.of(int.class));
            if (annotation.newVersion() == 0)
                this.checkParameterType(method, index++, TypeToken.of(int.class));
        }
        final ArrayList<TypeToken<?>> choices = new ArrayList<TypeToken<?>>(2);
        choices.add(this.byStorageIdType);
        choices.add(this.byNameType);
        this.checkParameterType(method, index++, choices);
        if (index != numParams) {
            throw new IllegalArgumentException(this.getErrorPrefix(method)
              + "method has " + (numParams - index) + " too many parameter(s)");
        }

        // Done
        return true;
    }

    @Override
    protected VersionChangeMethodInfo createMethodInfo(Method method, OnVersionChange annotation) {
        return new VersionChangeMethodInfo(method, annotation);
    }

// Comparator

    @Override
    public int compare(MethodInfo info1, MethodInfo info2) {
        final OnVersionChange annotation1 = info1.getAnnotation();
        final OnVersionChange annotation2 = info2.getAnnotation();
        int diff = Boolean.compare(annotation1.oldVersion() == 0, annotation2.oldVersion() == 0);
        if (diff != 0)
            return diff;
        diff = Boolean.compare(annotation1.newVersion() == 0, annotation2.newVersion() == 0);
        if (diff != 0)
            return diff;
        diff = info1.getMethod().getName().compareTo(info2.getMethod().getName());
        if (diff != 0)
            return diff;
        return 0;
    }

// VersionChangeMethodInfo

    class VersionChangeMethodInfo extends MethodInfo {

        private final boolean byName;

        @SuppressWarnings("unchecked")
        VersionChangeMethodInfo(Method method, OnVersionChange annotation) {
            super(method, annotation);
            final List<TypeToken<?>> actuals = OnVersionChangeScanner.this.getParameterTypeTokens(method);
            final TypeToken<?> oldValuesType = actuals.get(actuals.size() - 1);
            if (oldValuesType.equals(OnVersionChangeScanner.this.byStorageIdType))
                this.byName = false;
            else if (oldValuesType.equals(OnVersionChangeScanner.this.byNameType))
                this.byName = true;
            else
                throw new RuntimeException("internal error");
        }

        // Invoke method
        void invoke(JObject jobj, int oldVersion, int newVersion,
          Map<Integer, Object> oldValuesByStorageId, Map<String, Object> oldValuesByName) {

            // Get method info
            final OnVersionChange annotation = this.getAnnotation();
            final Method method = this.getMethod();

            // Check old & new version numbers
            if ((annotation.oldVersion() != 0 && annotation.oldVersion() != oldVersion)
              || (annotation.newVersion() != 0 && annotation.newVersion() != newVersion))
                return;

            // Determine which map to provide
            final Map<?, Object> oldValues = this.byName ? oldValuesByName : oldValuesByStorageId;

            // Figure out method parameters and invoke method
            if (annotation.oldVersion() != 0 && annotation.newVersion() != 0)
                Util.invoke(method, jobj, oldValues);
            else if (annotation.oldVersion() != 0)
                Util.invoke(method, jobj, newVersion, oldValues);
            else if (annotation.newVersion() != 0)
                Util.invoke(method, jobj, oldVersion, oldValues);
            else if (method.getParameterTypes().length == 1)        // special case where version numbers are completely ignored
                Util.invoke(method, jobj, oldValues);
            else
                Util.invoke(method, jobj, oldVersion, newVersion, oldValues);
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == this)
                return true;
            if (!super.equals(obj))
                return false;
            final OnVersionChangeScanner<?>.VersionChangeMethodInfo that = (OnVersionChangeScanner<?>.VersionChangeMethodInfo)obj;
            return this.byName == that.byName;
        }

        @Override
        public int hashCode() {
            return super.hashCode()
              ^ (this.byName ? 1 : 0);
        }
    }
}
