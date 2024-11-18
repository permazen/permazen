
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.util;

import com.google.common.base.Preconditions;
import com.google.common.reflect.TypeToken;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Type;
import java.lang.reflect.TypeVariable;
import java.lang.reflect.WildcardType;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Various utility routines relating to {@link TypeToken}s.
 */
public final class TypeTokens {

    private static final Method NEW_PARAMETERIZED_TYPE_METHOD;
    static {
        try {
            NEW_PARAMETERIZED_TYPE_METHOD = Class.forName(
                "com.google.common.reflect.Types", false, Thread.currentThread().getContextClassLoader())
              .getDeclaredMethod("newParameterizedType", Class.class, Type[].class);
            NEW_PARAMETERIZED_TYPE_METHOD.setAccessible(true);
        } catch (ClassNotFoundException | NoSuchMethodException e) {
            throw new RuntimeException("unexpected exception", e);
        }
    }

    private static final WildcardType QUESTION_MARK = new WildcardType() {

        @Override
        public Type[] getUpperBounds() {
            return new Type[] { Object.class };
        }

        @Override
        public Type[] getLowerBounds() {
            return new Type[0];
        }

        @Override
        public String toString() {
            return "?";
        }
    };

    private TypeTokens() {
    }

    /**
     * Find the narrowest type that is a supertype of all of the given types.
     *
     * <p>
     * This method delegates to {@link #findLowestCommonAncestor findLowestCommonAncestor()}
     * after converting the {@link Class} instances to {@link TypeToken}s.
     *
     * @param types sub-types
     * @return narrowest common super-type
     * @throws IllegalArgumentException if {@code types} or any type in {@code types} is null
     * @see #findLowestCommonAncestor findLowestCommonAncestor()
     */
    public static TypeToken<?> findLowestCommonAncestorOfClasses(Stream<Class<?>> types) {
        return TypeTokens.findLowestCommonAncestor(TypeTokens.noNulls(types, "type").map(TypeToken::of));
    }

    /**
     * Find the narrowest type(s) each of which is a supertype of all of the given types.
     *
     * <p>
     * This method delegates to {@link #findLowestCommonAncestors findLowestCommonAncestors()}
     * after converting the {@link Class} instances to {@link TypeToken}s.
     *
     * @param types sub-types
     * @return maximally narrow common supertype(s)
     * @throws IllegalArgumentException if any type in {@code types} is null
     * @see #findLowestCommonAncestors findLowestCommonAncestors()
     */
    public static Set<TypeToken<?>> findLowestCommonAncestorsOfClasses(Stream<Class<?>> types) {
        return TypeTokens.findLowestCommonAncestors(TypeTokens.noNulls(types, "type").map(TypeToken::of));
    }

    /**
     * Find the narrowest type that is a supertype of all of the given types.
     *
     * <p>
     * Note that there may be more than one such type. The returned type will always be as narrow
     * as possible, but it's possible there for there to be multiple such types for which none
     * is a sub-type of any other.
     *
     * @param types sub-types
     * @return narrowest common super-type
     */
    public static TypeToken<?> findLowestCommonAncestor(Stream<TypeToken<?>> types) {

        // Gather candidates
        final Set<TypeToken<?>> supertypes = TypeTokens.findLowestCommonAncestors(types);

        // Pick the best candidate that's not Object, if possible
        final TypeToken<Object> objectType = TypeToken.of(Object.class);
        supertypes.remove(objectType);
        switch (supertypes.size()) {
        case 0:
            return objectType;
        case 1:
            return supertypes.iterator().next();
        default:
            break;
        }

        // Pick the one that's not an interface, if any (it will be the the only non-interface type)
        for (TypeToken<?> supertype : supertypes) {
            if (!supertype.getRawType().isInterface())
                return supertype;
        }

        // There are now only mutually incompatible interfaces to choose from, so our last resort is Object
        return objectType;
    }

    /**
     * Find the narrowest type(s) each of which is a supertype of all of the given types.
     *
     * @param types sub-types
     * @return maximally narrow common supertype(s)
     */
    public static Set<TypeToken<?>> findLowestCommonAncestors(Stream<TypeToken<?>> types) {

        // Gather types, since we need to iterate over them multiple times
        final List<TypeToken<?>> typeList = types.collect(Collectors.toList());

        // Gather all supertypes of types recursively
        final HashSet<TypeToken<?>> supertypes = new HashSet<>();
        for (TypeToken<?> type : typeList)
            TypeTokens.addSupertypes(supertypes, type);

        // Throw out all supertypes that are not supertypes of every type
        for (Iterator<TypeToken<?>> i = supertypes.iterator(); i.hasNext(); ) {
            final TypeToken<?> supertype = i.next();
            for (TypeToken<?> type : typeList) {
                if (!supertype.isSupertypeOf(type)) {
                    i.remove();
                    break;
                }
            }
        }

        // Throw out all supertypes that are supertypes of some other supertype
        for (Iterator<TypeToken<?>> i = supertypes.iterator(); i.hasNext(); ) {
            final TypeToken<?> supertype = i.next();
            for (TypeToken<?> supertype2 : supertypes) {
                if (!supertype2.equals(supertype) && supertype.isSupertypeOf(supertype2)) {
                    i.remove();
                    break;
                }
            }
        }

        // Done
        return supertypes;
    }

    private static <T> Stream<T> noNulls(Stream<T> stream, String name) {
        return stream.peek(x -> {
            if (x == null)
                throw new IllegalArgumentException(String.format("null %s", name));
        });
    }

    @SuppressWarnings("unchecked")
    private static <T> void addSupertypes(Set<TypeToken<?>> types, TypeToken<T> type) {
        if (type == null || !types.add(type))
            return;
        final Class<? super T> rawType = type.getRawType();
        types.add(TypeToken.of(rawType));
        types.add(TypeTokens.getWildcardedType(rawType));
        final Class<? super T> superclass = rawType.getSuperclass();
        if (superclass != null) {
            TypeTokens.addSupertypes(types, TypeToken.of(superclass));
            TypeTokens.addSupertypes(types, type.getSupertype(superclass));
        }
        for (Class<?> iface : rawType.getInterfaces())
            TypeTokens.addSupertypes(types, type.getSupertype((Class<? super T>)iface));
    }

    /**
     * Parameterize the raw type with wildcards.
     *
     * @param type raw type
     * @param <T> raw type
     * @return {@code type} genericized with wildcards
     */
    public static <T> TypeToken<? extends T> getWildcardedType(Class<T> type) {
        Preconditions.checkArgument(type != null, "null type");
        final TypeVariable<Class<T>>[] typeVariables = type.getTypeParameters();
        if (typeVariables.length == 0)
            return TypeToken.of(type);
        final WildcardType[] questionMarks = new WildcardType[typeVariables.length];
        Arrays.fill(questionMarks, QUESTION_MARK);
        return TypeTokens.newParameterizedType(type, questionMarks);
    }

    /**
     * Convert a raw class back into its generic type using caller-supplied type parameters.
     *
     * @param target raw class
     * @param params type parameters
     * @param <T> raw class type
     * @return generic {@link TypeToken} for {@code target}
     * @see <a href="https://github.com/google/guava/issues/1645">Guava Issue #1645</a>
     */
    @SuppressWarnings("unchecked")
    public static <T> TypeToken<? extends T> newParameterizedType(Class<T> target, Type[] params) {
        Type type;
        try {
            type = (Type)TypeTokens.NEW_PARAMETERIZED_TYPE_METHOD.invoke(null, target, params);
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new RuntimeException("unexpected exception", e instanceof InvocationTargetException ? e.getCause() : e);
        }
        return (TypeToken<T>)TypeToken.of(type);
    }
}
