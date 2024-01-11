
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen;

import com.google.common.reflect.TypeParameter;
import com.google.common.reflect.TypeToken;

import io.permazen.annotation.PermazenField;
import io.permazen.annotation.PermazenSetField;

import java.lang.annotation.Annotation;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.NavigableSet;
import java.util.Optional;
import java.util.Set;

/**
 * Scans for {@link io.permazen.annotation.ReferencePath &#64;ReferencePath} annotations.
 */
class ReferencePathScanner<T> extends AnnotationScanner<T, io.permazen.annotation.ReferencePath> {

    private static final List<Class<? extends Annotation>> CONFLICT_CLASSES = new ArrayList<>();
    static {
        CONFLICT_CLASSES.add(PermazenField.class);
        CONFLICT_CLASSES.add(PermazenSetField.class);
    }

    ReferencePathScanner(PermazenClass<T> pclass) {
        super(pclass, io.permazen.annotation.ReferencePath.class);
    }

    @SuppressWarnings("unchecked")
    public Set<ReferencePathMethodInfo> findReferencePathMethods() {
        return (Set<ReferencePathMethodInfo>)(Object)this.findAnnotatedMethods();
    }

    @Override
    protected boolean includeMethod(Method method, io.permazen.annotation.ReferencePath annotation) {
        this.checkNotStatic(method);
        this.checkParameterTypes(method);
        return true;
    }

    @Override
    protected ReferencePathMethodInfo createMethodInfo(Method method, io.permazen.annotation.ReferencePath annotation) {
        return new ReferencePathMethodInfo(method, annotation);
    }

    // This method exists solely to bind the generic type parameters
    @SuppressWarnings("serial")
    private static <E> TypeToken<NavigableSet<E>> buildNavigableSetType(Class<E> elementType) {
        return new TypeToken<NavigableSet<E>>() { }.where(new TypeParameter<E>() { }, elementType);
    }

    // This method exists solely to bind the generic type parameters
    @SuppressWarnings("serial")
    private static <E> TypeToken<Optional<E>> buildOptionalType(Class<E> elementType) {
        return new TypeToken<Optional<E>>() { }.where(new TypeParameter<E>() { }, elementType);
    }

// ReferencePathMethodInfo

    class ReferencePathMethodInfo extends MethodInfo {

        private final ReferencePath path;
        private final boolean returnsOptional;

        ReferencePathMethodInfo(Method method, io.permazen.annotation.ReferencePath annotation) {
            super(method, annotation);

            // Check for conflict with method having @ReferencePath and @PermazenField or @PermazenSetField
            final String errorPrefix = ReferencePathScanner.this.getErrorPrefix(method);
            for (Class<? extends Annotation> conflictClass : CONFLICT_CLASSES) {
                if (Util.getAnnotation(method, conflictClass) != null) {
                    throw new IllegalArgumentException(String.format("%smethod has conflicting annotations @%s and @%s",
                      conflictClass.getSimpleName(), io.permazen.annotation.ReferencePath.class.getSimpleName()));
                }
            }

            // Parse reference path
            final Class<T> modelType = ReferencePathScanner.this.pclass.type;

            // Parse reference path
            try {
                this.path = ReferencePathScanner.this.pclass.pdb.parseReferencePath(modelType, annotation.value());
            } catch (IllegalArgumentException e) {
                throw new IllegalArgumentException(String.format(
                  "%s: invalid @%s reference path: %s", errorPrefix,
                  io.permazen.annotation.ReferencePath.class.getSimpleName(), e.getMessage()), e);
            }

            // Verify return type
            ReferencePathScanner.this.checkReturnType(method, this.path.isSingular() ?
              new Class<?>[] { Optional.class } : new Class<?>[] { Optional.class, NavigableSet.class });
            this.returnsOptional = method.getReturnType().equals(Optional.class);

            // Check method return type: generic type parameter should be a super-type of all possible target types
            final TypeToken<?> returnType = TypeToken.of(method.getGenericReturnType());
            final TypeToken<?> returnElementType = returnType.resolveType(method.getReturnType().getTypeParameters()[0]);
            for (PermazenClass<?> actualElementType : this.path.getTargetTypes()) {
                final Class<?> type = actualElementType != null ? actualElementType.type : UntypedPermazenObject.class;
                if (!returnElementType.isSupertypeOf(type)) {
                    throw new IllegalArgumentException(String.format(
                      "%s: method return element type %s is not compatible with %s",
                      errorPrefix, returnElementType, type));
                }
            }
        }

        public ReferencePath getReferencePath() {
            return this.path;
        }

        public boolean isReturnsOptional() {
            return this.returnsOptional;
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == this)
                return true;
            if (!super.equals(obj))
                return false;
            final ReferencePathScanner<?>.ReferencePathMethodInfo that = (ReferencePathScanner<?>.ReferencePathMethodInfo)obj;
            return this.path.equals(that.path);
        }

        @Override
        public int hashCode() {
            return super.hashCode() ^ this.path.hashCode();
        }
    }
}
