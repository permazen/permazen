
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen;

import com.google.common.reflect.TypeToken;

import io.permazen.annotation.OnCreate;
import io.permazen.core.CreateListener;
import io.permazen.core.ObjId;
import io.permazen.core.Transaction;

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Scans for {@link OnCreate &#64;OnCreate} annotations.
 */
class OnCreateScanner<T> extends AnnotationScanner<T, OnCreate> {

    OnCreateScanner(PermazenClass<T> pclass) {
        super(pclass, OnCreate.class);
    }

    @Override
    protected boolean includeMethod(Method method, OnCreate annotation) {
        this.checkReturnType(method, void.class);
        return true;                                    // we do further parameter type check in CreateMethodInfo
    }

    @Override
    protected CreateMethodInfo createMethodInfo(Method method, OnCreate annotation) {
        return new CreateMethodInfo(method, annotation);
    }

// CreateMethodInfo

    class CreateMethodInfo extends MethodInfo implements CreateListener {

        final List<? extends PermazenClass<?>> pclasses;
        final InvokeStyle invokeStyle;

        CreateMethodInfo(Method method, OnCreate annotation) {
            super(method, annotation);

            // Gather info
            final String errorPrefix = OnCreateScanner.this.getErrorPrefix(method);
            final List<TypeToken<?>> paramGenTypes = OnCreateScanner.this.getParameterTypeTokens(method);
            final int numParams = paramGenTypes.size();
            final boolean staticMethod = (method.getModifiers() & Modifier.STATIC) != 0;
            final boolean selfMatch = !staticMethod && numParams == 0;

            // Check number of parameters
            if (numParams != 1 && !selfMatch) {
                throw new IllegalArgumentException(String.format(
                  "%s: @%s method is required to have exactly one parameter",
                  errorPrefix, annotation.annotationType().getSimpleName()));
            }

            // Determine method invocation style and accepted object type
            final Class<?> acceptedRawType;
            final TypeToken<?> acceptedGenType;
            if (selfMatch) {
                acceptedGenType = TypeToken.of(method.getDeclaringClass());
                this.invokeStyle = (ptx, deleted) -> Util.invoke(method, deleted);
            } else {
                acceptedGenType = paramGenTypes.get(0);
                if (staticMethod)
                    this.invokeStyle = (ptx, deleted) -> Util.invoke(method, null, deleted);
                else
                    this.invokeStyle = (ptx, deleted) -> Util.invoke(method, deleted, deleted);
            }
            acceptedRawType = acceptedGenType.getRawType();

            // Find matching object types
            this.pclasses = OnCreateScanner.this.pclass.pdb.getPermazenClasses(acceptedRawType);
            if (this.pclasses.isEmpty()) {
                throw new IllegalArgumentException(String.format(
                  "%s: there are no object types matching the method's parameter type %s",
                  errorPrefix, acceptedGenType));
            }
            final List<TypeToken<?>> targetGenTypes = this.pclasses.stream()
              .map(pclass -> TypeToken.of(pclass.type))
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

        // Register listeners for this method
        void registerCreateListeners(Transaction tx) {
            this.pclasses.stream()
              .forEach(pclass -> tx.addCreateListener(pclass.storageId, this));
        }

        // Note my fields are derived from this.method, so there's no need to include them in equals() or hashCode()

    // CreateListener

        @Override
        public void onCreate(Transaction tx, ObjId id) {

            // Get our transaction
            final PermazenTransaction ptx = (PermazenTransaction)tx.getUserObject();
            assert ptx != null && ptx.tx == tx;

            // Get the new object
            final PermazenObject created = ptx.get(id);

            // Invoke listener method
            this.invokeStyle.invoke(ptx, created);
        }
    }

// InvokeStyle

    @FunctionalInterface
    private interface InvokeStyle {
        void invoke(PermazenTransaction ptx, PermazenObject deleted);
    }
}
