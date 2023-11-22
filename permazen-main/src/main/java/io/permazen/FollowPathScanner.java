
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen;

import com.google.common.reflect.TypeParameter;
import com.google.common.reflect.TypeToken;

import io.permazen.annotation.FollowPath;
import io.permazen.annotation.JField;
import io.permazen.annotation.JSetField;

import java.lang.annotation.Annotation;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.NavigableSet;
import java.util.Optional;

/**
 * Scans for {@link FollowPath &#64;FollowPath} annotations.
 */
class FollowPathScanner<T> extends AnnotationScanner<T, FollowPath> {

    private static final List<Class<? extends Annotation>> CONFLICT_CLASSES = new ArrayList<>();
    static {
        CONFLICT_CLASSES.add(JField.class);
        CONFLICT_CLASSES.add(JSetField.class);
    }

    FollowPathScanner(JClass<T> jclass) {
        super(jclass, FollowPath.class);
    }

    @Override
    protected boolean includeMethod(Method method, FollowPath followPath) {
        this.checkNotStatic(method);
        this.checkParameterTypes(method);
        return true;
    }

    @Override
    protected FollowPathMethodInfo createMethodInfo(Method method, FollowPath annotation) {
        return new FollowPathMethodInfo(method, annotation);
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

// FollowPathMethodInfo

    class FollowPathMethodInfo extends MethodInfo {

        private final ReferencePath path;
        private final boolean returnsOptional;

        FollowPathMethodInfo(Method method, FollowPath followPath) {
            super(method, followPath);

            // Check for conflict with method having @FollowPath and @JField or @JSetField
            final String errorPrefix = FollowPathScanner.this.getErrorPrefix(method);
            for (Class<? extends Annotation> conflictClass : CONFLICT_CLASSES) {
                if (Util.getAnnotation(method, conflictClass) != null) {
                    throw new IllegalArgumentException(String.format("%smethod has conflicting annotations @%s and @%s",
                      conflictClass.getSimpleName(), FollowPath.class.getSimpleName()));
                }
            }

            // Parse reference path
            final Class<T> modelType = FollowPathScanner.this.jclass.type;

            // Parse reference path
            try {
                this.path = FollowPathScanner.this.jclass.jdb.parseReferencePath(modelType, followPath.value());
            } catch (IllegalArgumentException e) {
                throw new IllegalArgumentException(String.format(
                  "%s: invalid @%s reference path: %s", errorPrefix, FollowPath.class.getSimpleName(), e.getMessage()), e);
            }

            // Verify return type
            FollowPathScanner.this.checkReturnType(method, this.path.isSingular() ?
              new Class<?>[] { Optional.class } : new Class<?>[] { Optional.class, NavigableSet.class });
            this.returnsOptional = method.getReturnType().equals(Optional.class);

            // Check method return type: generic type parameter should be a super-type of all possible target types
            final TypeToken<?> returnType = TypeToken.of(method.getGenericReturnType());
            final TypeToken<?> returnElementType = returnType.resolveType(method.getReturnType().getTypeParameters()[0]);
            for (JClass<?> actualElementType : this.path.getTargetTypes()) {
                final Class<?> type = actualElementType != null ? actualElementType.type : UntypedJObject.class;
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
            final FollowPathScanner<?>.FollowPathMethodInfo that = (FollowPathScanner<?>.FollowPathMethodInfo)obj;
            return this.path.equals(that.path);
        }

        @Override
        public int hashCode() {
            return super.hashCode() ^ this.path.hashCode();
        }
    }
}
