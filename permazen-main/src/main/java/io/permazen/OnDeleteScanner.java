
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen;

import com.google.common.reflect.TypeToken;

import io.permazen.annotation.OnDelete;
import io.permazen.core.DeleteListener;
import io.permazen.core.ObjId;
import io.permazen.core.Transaction;

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.List;
import java.util.NavigableSet;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Scans for {@link OnDelete &#64;OnDelete} annotations.
 */
class OnDeleteScanner<T> extends AnnotationScanner<T, OnDelete> {

    OnDeleteScanner(PermazenClass<T> pclass) {
        super(pclass, OnDelete.class);
    }

    @Override
    protected boolean includeMethod(Method method, OnDelete annotation) {
        this.checkReturnType(method, void.class);
        return true;                                    // we do further parameter type check in DeleteMethodInfo
    }

    @Override
    protected DeleteMethodInfo createMethodInfo(Method method, OnDelete annotation) {
        return new DeleteMethodInfo(method, annotation);
    }

// DeleteMethodInfo

    class DeleteMethodInfo extends MethodInfo implements DeleteListener {

        final ReferencePath path;
        final Class<?> acceptedRawType;
        final InvokeStyle invokeStyle;

        DeleteMethodInfo(Method method, OnDelete annotation) {
            super(method, annotation);

            // Gather info
            final String errorPrefix = OnDeleteScanner.this.getErrorPrefix(method);
            final List<TypeToken<?>> paramGenTypes = OnDeleteScanner.this.getParameterTypeTokens(method);
            final int numParams = paramGenTypes.size();

            // Determine method invocation style and accepted object type
            final TypeToken<?> acceptedGenType;
            if ((method.getModifiers() & Modifier.STATIC) != 0) {
                if (numParams != 1) {
                    throw new IllegalArgumentException(String.format(
                      "%s: a static @%s method is required to have exactly one parameter",
                      errorPrefix, annotation.annotationType().getSimpleName()));
                }
                this.invokeStyle = (ptx, deleted, referrers) -> Util.invoke(method, null, deleted);
                acceptedGenType = paramGenTypes.get(0);
            } else if (annotation.path().isEmpty()) {
                if (numParams != 0) {
                    throw new IllegalArgumentException(String.format(
                      "%s: @%s.path() is empty so method is required to have zero parameters",
                      errorPrefix, annotation.annotationType().getSimpleName()));
                }
                this.invokeStyle = (ptx, deleted, referrers) -> {
                    assert referrers.size() == 1;
                    assert referrers.first().equals(deleted.getObjId());
                    Util.invoke(method, deleted);
                };
                acceptedGenType = TypeToken.of(method.getDeclaringClass());
            } else {
                if (numParams != 1) {
                    throw new IllegalArgumentException(String.format(
                      "%s: @%s.path() is non-empty so method is required to have exactly one parameter",
                      errorPrefix, annotation.annotationType().getSimpleName()));
                }
                this.invokeStyle = (ptx, deleted, referrers) -> {
                    referrers.stream()
                      .map(ptx::get)
                      .forEach(referrer -> Util.invoke(method, referrer, deleted));
                };
                acceptedGenType = paramGenTypes.get(0);
            }
            this.acceptedRawType = acceptedGenType.getRawType();

            // Parse reference path
            try {
                this.path = OnDeleteScanner.this.pclass.pdb.parseReferencePath(method.getDeclaringClass(), annotation.path());
            } catch (IllegalArgumentException e) {
                throw new IllegalArgumentException(String.format("%s: %s", errorPrefix, e.getMessage()), e);
            }

            // Validate parameter type
            if (numParams > 0) {

                // Verify at least one target type is accepted by the method
                final Set<Class<?>> targetRawTypes = path.getTargetTypes().stream()
                  .map(pclass -> Optional.ofNullable(pclass)
                    .<Class<?>>map(PermazenClass::getType)
                    .orElse(UntypedPermazenObject.class))
                  .collect(Collectors.toSet());
                if (targetRawTypes.stream().noneMatch(this.acceptedRawType::isAssignableFrom)) {
                    throw new IllegalArgumentException(String.format(
                      "%s: there are no target object types matching the method's parameter type %s",
                      errorPrefix, acceptedGenType));
                }
                final List<TypeToken<?>> targetGenTypes = targetRawTypes.stream()
                  .map(TypeToken::of)
                  .collect(Collectors.toList());

                // Verify the method accepts object types consistently whether raw vs. generic
                final TypeToken<?> mismatchType = Util.findErasureDifference(acceptedGenType, targetGenTypes);
                if (mismatchType != null) {
                    throw new IllegalArgumentException(String.format(
                      "%s: parameter type %s matches objects of type %s due to type erasure,"
                      + " but its generic type is does not match %s; try narrowing or"
                      + " widening the parameter type while keeping it compatible with %s",
                      errorPrefix, acceptedGenType, mismatchType, mismatchType,
                      targetGenTypes.size() != 1 ?  "one or more of: " + targetGenTypes : targetGenTypes.get(0)));
                }
            }
        }

        // Register listeners for this method
        void registerDeleteListener(Transaction tx) {
            tx.addDeleteListener(path.getReferenceFields(), path.getPathKeyRanges(), this);
        }

        // Note genericTypes is derived from this.method, so there's no need to include it in equals() or hashCode()
        @Override
        public boolean equals(Object obj) {
            if (obj == this)
                return true;
            if (!super.equals(obj))
                return false;
            final OnDeleteScanner<?>.DeleteMethodInfo that = (OnDeleteScanner<?>.DeleteMethodInfo)obj;
            return this.path.equals(that.path);
        }

        @Override
        public int hashCode() {
            return super.hashCode()
              ^ this.path.hashCode();
        }

    // DeleteListener

        @Override
        public void onDelete(Transaction tx, ObjId id, int[] path, NavigableSet<ObjId> referrers) {

            // Get our transaction
            final PermazenTransaction ptx = (PermazenTransaction)tx.getUserObject();
            assert ptx != null && ptx.tx == tx;

            // Is the deleted object type's compatible with what the target method?
            final PermazenObject deleted = ptx.get(id);
            if (!this.acceptedRawType.isInstance(deleted))
                return;

            // Invoke listener method
            this.invokeStyle.invoke(ptx, deleted, referrers);
        }
    }

// InvokeStyle

    @FunctionalInterface
    private interface InvokeStyle {
        void invoke(PermazenTransaction ptx, PermazenObject deleted, NavigableSet<ObjId> referrers);
    }
}
