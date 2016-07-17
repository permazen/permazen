
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb.parse.expr;

import com.google.common.base.Preconditions;
import com.google.common.reflect.TypeToken;

import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.lang.reflect.Type;
import java.lang.reflect.TypeVariable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;

/**
 * Method lookup utility class.
 */
final class MethodUtil {

    private MethodUtil() {
    }

    /**
     * Find the accessible method with the specified name that's compatible with the given signature info.
     *
     * <p>
     * If any element in {@code paramTypes} is {@link FunctionalType}, then that parameter matches any functional type.
     * If any element in {@code paramTypes} is {@link NullType}, then that parameter matches any non-primitive type.
     *
     * <p>
     * If {@code returnType} is null, then any return type matches.
     *
     * <p>
     * Note: this does not correctly handle all of the oddball corner cases as specified by the JLS.
     *
     * @param type class to search
     * @param name method name
     * @param paramTypes parameter type lower bounds; may contain {@link FunctionalType} and {@link NullType}
     * @param returnType return type upper bound, or null for don't care
     * @param isStatic true to search static methods, false to search instance methods
     * @return the matching method
     * @throws EvalException if exactly one matching method is not found
     */
    public static Method findMatchingMethod(Class<?> type, String name, Type[] paramTypes, Class<?> returnType, boolean isStatic) {

        // Find method
        final ArrayList<MethodExecutable> methods = new ArrayList<>();
        for (Method method : type.getMethods()) {
            if (((method.getModifiers() & Modifier.STATIC) != 0) == isStatic)
                methods.add(new MethodExecutable(method));
        }
        final Method method = MethodUtil.findMatchingExecutable(type, methods,
          name, paramTypes, returnType, (isStatic ? "static" : "instance") + " method").getMember();

        // Done
        try {
            return MethodUtil.makeAccessible(method);
        } catch (IllegalArgumentException e) {
            throw new EvalException(e.getMessage());
        }
    }

    /**
     * Find the accessible constructor that's compatible with the given signature info.
     *
     * <p>
     * If any element in {@code paramTypes} is {@link FunctionalType}, then that parameter matches any functional type.
     * If any element in {@code paramTypes} is {@link NullType}, then that parameter matches any non-primitive type.
     *
     * <p>
     * If {@code returnType} is null, then any return type matches.
     *
     * <p>
     * Note: this does not correctly handle all of the oddball corner cases as specified by the JLS.
     *
     * @param type class to search
     * @param paramTypes parameter type lower bounds; may contain {@link FunctionalType} and {@link NullType}
     * @return the matching constructor
     * @throws EvalException if exactly one matching constructor is not found
     */
    public static Constructor<?> findMatchingConstructor(Class<?> type, Type[] paramTypes) {
        final ArrayList<ConstructorExecutable> constructors = new ArrayList<>();
        for (Constructor<?> constructor : type.getConstructors())
            constructors.add(new ConstructorExecutable(constructor));
        return MethodUtil.findMatchingExecutable(type,
          constructors, type.getName(), paramTypes, null, "constructor").getMember();
    }

    /**
     * Find the accessible member with the specified name that's compatible with the given signature info.
     *
     * <p>
     * If any element in {@code paramTypes} is {@link FunctionalType}, then that parameter matches any functional type.
     * If any element in {@code paramTypes} is {@link NullType}, then that parameter matches any non-primitive type.
     *
     * <p>
     * If {@code returnType} is null, then any return type matches.
     *
     * <p>
     * Note: this is a hack and not JLS-compliant.
     *
     * @param type containing type
     * @param executables items to search
     * @param name member name, or null to match all
     * @param paramTypes parameter type lower bounds; may contain {@link FunctionalType} and {@link NullType}
     * @param returnType return type upper bound, or null for don't care
     * @param description description of member
     * @return the matching member
     * @throws EvalException if exactly one matching member is not found
     */
    private static <T extends Executable<?>> T findMatchingExecutable(Class<?> type, Iterable<? extends T> executables,
      String name, Type[] paramTypes, Class<?> returnType, String description) {

        // Gather candidates
        final ArrayList<T> candidates = new ArrayList<>(3);
    nextMethod:
        for (T executable : executables) {

            // Check name
            if (name != null && !executable.getName().equals(name))
                continue;

            // Check return type
            if (returnType != null && !MethodUtil.isCompatible(returnType, executable.getReturnType()))
                continue;

            // Check parameter count
            final Class<?>[] mparamTypes = executable.getParameterTypes();
            if (!executable.isVarArgs()) {
                if (mparamTypes.length != paramTypes.length)
                    continue;
            } else {
                if (mparamTypes.length > paramTypes.length + 1)
                    continue;
            }

            // Check parameter types
            for (int i = 0; i < paramTypes.length; i++) {

                // Handle varargs
                if (executable.isVarArgs() && i >= mparamTypes.length - 1) {
                    final Class<?> lastParamType = mparamTypes[mparamTypes.length - 1];

                    // Check if non-varargs invocation is possible
                    if (paramTypes.length == mparamTypes.length
                      && MethodUtil.isCompatibleMethodParam(mparamTypes[i], paramTypes[i]))
                        continue;

                    // Check if varargs invocation is possible
                    assert lastParamType.isArray();
                    if (MethodUtil.isCompatibleMethodParam(lastParamType.getComponentType(), paramTypes[i]))
                        continue;

                    // Does not match
                    continue nextMethod;
                }

                // Handle regular parameter match
                if (!MethodUtil.isCompatibleMethodParam(mparamTypes[i], paramTypes[i]))
                    continue nextMethod;
            }

            // We found a candidate
            candidates.add(executable);
        }

        // Any matches?
        if (candidates.isEmpty())
            throw new EvalException("no compatible " + description + " `" + name + "()' found in " + type);

        // Find "best" candidate, if any
        try {
            Collections.sort(candidates, new SignatureComparator());
        } catch (IllegalArgumentException e) {
            throw new EvalException("ambiguous invocation of `" + name + "()' in " + type);
        }

        // Done
        return candidates.get(0);
    }

    public static boolean isCompatibleMethodParam(Class<?> to, Type from) {

        // Match type-inferring nodes against functional types
        if (from == FunctionalType.class) {
            try {
                MethodUtil.findFunctionalMethod(to);
            } catch (EvalException e) {
                return false;
            }
            return true;
        }

        // Match null type
        if (from == NullType.class)
            return !to.isPrimitive();

        // If parameter is a generic type variable, assume it can be any narrower type
        if (from instanceof TypeVariable) {
            for (Type bound : ((TypeVariable)from).getBounds()) {
                if (!TypeToken.of(bound).isAssignableFrom(TypeToken.of(to).wrap()))
                    return false;
            }
            return true;
        }

        // Handle default case
        return MethodUtil.isCompatible(to, TypeToken.of(from).getRawType());
    }

    public static boolean isCompatible(Class<?> to, Class<?> from) {
        if (to.isPrimitive()) {
            if (!(from = TypeToken.of(from).unwrap().getRawType()).isPrimitive())
                return false;
            return MethodUtil.isCompatiblePrimitive(to, from);
        }
        if (from.isPrimitive())
            from = TypeToken.of(from).wrap().getRawType();
        return to.isAssignableFrom(from);
    }

    public static boolean isCompatiblePrimitive(Class<?> to, Class<?> from) {
        assert to.isPrimitive();
        assert from.isPrimitive();
        if (to == from)
            return true;
        if (to == int.class) {
            return from.equals(byte.class)
              || from.equals(char.class) || from.equals(short.class);
        }
        if (to == float.class || to == long.class) {
            return from.equals(byte.class)
              || from.equals(char.class) || from.equals(short.class)
              || from.equals(int.class);
        }
        if (to == double.class) {
            return from.equals(byte.class)
              || from.equals(char.class) || from.equals(short.class)
              || from.equals(int.class) || from.equals(float.class)
              || from.equals(long.class);
        }
        return false;
    }

    /**
     * Find the (one) abstract method in the specified functional interface type.
     *
     * @param type functional type
     * @return the unimplemented method in {@code type}
     * @throws EvalException if {@code type} is not an interface type
     * @throws EvalException if {@code type} is not a functional interface
     * @throws IllegalArgumentException if {@code type} is null
     */
    public static Method findFunctionalMethod(Class<?> type) {
        if (!type.isInterface())
            throw new EvalException(type + " is not an interface type");
        Method functionalMethod = null;
        for (Method method : type.getMethods()) {
            if ((method.getModifiers() & Modifier.ABSTRACT) == 0 || MethodUtil.isPublicObjectMethod(method))
                continue;
            if (functionalMethod != null) {
                functionalMethod = null;
                break;
            }
            functionalMethod = method;
        }
        if (functionalMethod == null)
            throw new EvalException(type + " is not a functional type");
        return functionalMethod;
    }

    /**
     * Return a publicly accessible variant of the given {@link Method}, if any.
     *
     * <p>
     * This is a workaround for the problem where non-public class C implements public method M of interface I.
     * In that case, invoking method C.M results in IllegalAccessException; instead, you have to invoke I.M.
     *
     * @param method the method
     * @throws IllegalArgumentException if {@code method} is null
     * @throws IllegalArgumentException if no public version of {@code method} exists
     */
    static Method makeAccessible(Method method) {
        Preconditions.checkArgument(method != null, "null method");
        Class<?> cl = method.getDeclaringClass();
        do {
            if ((cl.getModifiers() & Modifier.PUBLIC) != 0) {
                try {
                    return cl.getMethod(method.getName(), method.getParameterTypes());
                } catch (NoSuchMethodException e) {
                    // ignore
                }
            }
            for (Class<?> iface : cl.getInterfaces()) {
                try {
                    return iface.getMethod(method.getName(), method.getParameterTypes());
                } catch (NoSuchMethodException e) {
                    continue;
                }
            }
        } while ((cl = cl.getSuperclass()) != null);
        throw new IllegalArgumentException("method " + method + " has no public variant");
    }

    static boolean isPublicObjectMethod(Method method) {
        final Method objMethod;
        try {
            objMethod = Object.class.getDeclaredMethod(method.getName(), method.getParameterTypes());
        } catch (NoSuchMethodException e) {
            return false;
        }
        return (objMethod.getModifiers() & Modifier.PUBLIC) != 0;
    }

// NullType

    public static final class NullType {
        private NullType() {
        }
    }

// FunctionalType

    public static final class FunctionalType {
        private FunctionalType() {
        }
    }

// SignatureComparator

    private static class SignatureComparator implements Comparator<Executable<?>> {

        @Override
        public int compare(Executable<?> m1, Executable<?> m2) {
            final Class<?>[] types1 = m1.getParameterTypes();
            final Class<?>[] types2 = m2.getParameterTypes();
            if (types1.length != types2.length)
                throw new IllegalArgumentException();
            boolean m1wide = true;
            boolean m2wide = true;
            for (int i = 0; i < types1.length; i++) {
                if (!MethodUtil.isCompatibleMethodParam(types1[i], types2[i]))
                    m1wide = false;
                if (!MethodUtil.isCompatibleMethodParam(types2[i], types1[i]))
                    m2wide = false;
            }
            if (m1wide && m2wide) {
                if (m1 instanceof MethodExecutable && m2 instanceof MethodExecutable) {     // choose narrowest method return type
                    final Class<?> rtype1 = ((MethodExecutable)m1).getMember().getReturnType();
                    final Class<?> rtype2 = ((MethodExecutable)m2).getMember().getReturnType();
                    final boolean r1narrow = rtype2.isAssignableFrom(rtype1);
                    final boolean r2narrow = rtype1.isAssignableFrom(rtype2);
                    if (r1narrow && !r2narrow)
                        return -1;
                    if (!r1narrow && r2narrow)
                        return 1;
                }
                return 0;
            }
            if (m1wide && !m2wide)
                return 1;
            if (!m1wide && m2wide)
                return -1;
            throw new IllegalArgumentException();
        }
    }
}
