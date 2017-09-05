
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen;

import com.google.common.reflect.TypeParameter;
import com.google.common.reflect.TypeToken;

import io.permazen.annotation.FollowPath;
import io.permazen.annotation.JSetField;

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.NavigableSet;
import java.util.Optional;

/**
 * Scans for {@link FollowPath &#64;FollowPath} annotations.
 */
class FollowPathScanner<T> extends AnnotationScanner<T, FollowPath> {

    FollowPathScanner(JClass<T> jclass) {
        super(jclass, FollowPath.class);
    }

    @Override
    protected boolean includeMethod(Method method, FollowPath followPath) {
        this.checkNotStatic(method);
        this.checkParameterTypes(method);
        this.checkReturnType(method, followPath.firstOnly() ? Optional.class : NavigableSet.class);
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
        private final boolean inverse;

        FollowPathMethodInfo(Method method, FollowPath followPath) {
            super(method, followPath);

            // Check for conflict with method having both @JSetField and @FollowPath
            if (Util.getAnnotation(method, JSetField.class) != null) {
                throw new IllegalArgumentException(FollowPathScanner.this.getErrorPrefix(method)
                  + "method has conflicting annotations with both @" + JSetField.class.getSimpleName()
                  + " and @" + FollowPath.class.getSimpleName());
            }

            // Parse reference path
            final Class<T> modelType = FollowPathScanner.this.jclass.type;
            this.inverse = followPath.startingFrom() != void.class;
            try {

                // Check for annotation property conflict
                if ((!this.inverse && (followPath.value().equals("") || !followPath.inverseOf().equals("")))
                  || (this.inverse && (!followPath.value().equals("") || followPath.inverseOf().equals("")))) {
                    throw new IllegalArgumentException(
                      "invalid property combination: either value() or both startingFrom() and inverseOf() should be specified");
                }

                // Parse reference path
                this.path = FollowPathScanner.this.jclass.jdb.parseReferencePath(
                  this.inverse ? followPath.startingFrom() : modelType,
                  this.inverse ? followPath.inverseOf() : followPath.value(), false);
            } catch (IllegalArgumentException e) {
                throw new IllegalArgumentException(FollowPathScanner.this.getErrorPrefix(method)
                  + "invalid reference path: " + e.getMessage(), e);
            }

            // Check method return type: element type should be a super-type of all possible target types
            if (this.inverse) {

                // Check return type
                final TypeToken<?> expectedType = followPath.firstOnly() ?
                  FollowPathScanner.buildOptionalType(followPath.startingFrom()) :
                  FollowPathScanner.buildNavigableSetType(followPath.startingFrom());
                FollowPathScanner.this.checkReturnType(method, Arrays.asList(expectedType));

                // Check reference path can possibly end in 'this'
                boolean matched = false;
                for (Class<?> targetType : this.path.getTargetTypes()) {
                    if (targetType.isAssignableFrom(modelType)) {
                        matched = true;
                        break;
                    }
                }
                if (!matched) {
                    throw new IllegalArgumentException(FollowPathScanner.this.getErrorPrefix(method)
                      + "inverted reference path can never terminate in an instance of " + modelType);
                }
            } else {

                // Check return type
                final TypeToken<?> returnType = TypeToken.of(method.getGenericReturnType());
                final TypeToken<?> returnElementType =
                  returnType.resolveType((followPath.firstOnly() ? Optional.class : NavigableSet.class).getTypeParameters()[0]);
                for (Class<?> actualElementType : this.path.getTargetTypes()) {
                    if (!returnElementType.isSupertypeOf(actualElementType)) {
                        throw new IllegalArgumentException(FollowPathScanner.this.getErrorPrefix(method)
                          + "return type element type " + returnElementType + " is not compatible with " + actualElementType);
                    }
                }
            }
        }

        public ReferencePath getReferencePath() {
            return path;
        }

        public boolean isInverse() {
            return this.inverse;
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == this)
                return true;
            if (!super.equals(obj))
                return false;
            final FollowPathScanner<?>.FollowPathMethodInfo that = (FollowPathScanner<?>.FollowPathMethodInfo)obj;
            return this.path.equals(that.path) && this.inverse == that.inverse;
        }

        @Override
        public int hashCode() {
            return super.hashCode() ^ this.path.hashCode() ^ Boolean.hashCode(this.inverse);
        }
    }
}

