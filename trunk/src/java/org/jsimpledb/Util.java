
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb;

import com.google.common.reflect.TypeToken;

import java.lang.annotation.Annotation;
import java.lang.reflect.AnnotatedElement;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.lang.reflect.TypeVariable;
import java.lang.reflect.WildcardType;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.validation.Constraint;

/**
 * Utility routines;
 */
final class Util {

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

    private static Method newParameterizedTypeMethod;

    private Util() {
    }

    /**
     * Determine if there is an annotation on {@code thing} with a {@link Constraint} meta-annotation.
     */
    public static boolean requiresValidation(AnnotatedElement thing) {
        if (thing == null)
            throw new IllegalArgumentException("null thing");
        for (Annotation annotation : thing.getAnnotations()) {
            final Class<?> atype = annotation.annotationType();
            if (atype.isAnnotationPresent(Constraint.class))
                return true;
        }
        return false;
    }

    /**
     * Find the setter method corresponding to a getter method.
     */
    public static Method findSetterMethod(TypeToken<?> type, Method getter) {
        final Matcher matcher = Pattern.compile("(is|get)(.+)").matcher(getter.getName());
        if (!matcher.matches()) {
            throw new IllegalArgumentException("can't infer setter method name from getter method "
              + getter.getName() + "() because name does not follow Java bean naming conventions");
        }
        final String setterName = "set" + matcher.group(2);
        for (TypeToken<?> superType : type.getTypes()) {
            try {
                final Method setter = superType.getRawType().getMethod(setterName, getter.getReturnType());
                if (setter.getReturnType() != Void.TYPE)
                    continue;
                return setter;
            } catch (NoSuchMethodException e) {
                continue;
            }
        }
        throw new IllegalArgumentException("can't find any setter method " + setterName
          + "() corresponding to getter method " + getter.getName() + "() taking " + getter.getReturnType()
          + " and returning void");
    }

    /**
     * Find the narrowest type that is a supertype of all of the given types.
     */
    public static TypeToken<?> findLowestCommonAncestor(Iterable<TypeToken<?>> types) {

        // Gather all supertypes of types recursively
        final HashSet<TypeToken<?>> supertypes = new HashSet<>();
        for (TypeToken<?> type : types)
            Util.addSupertypes(supertypes, type);

        // Throw out all supertypes that are not supertypes of every type
        for (Iterator<TypeToken<?>> i = supertypes.iterator(); i.hasNext(); ) {
            final TypeToken<?> supertype = i.next();
            for (TypeToken<?> type : types) {
                if (!supertype.isAssignableFrom(type)) {
                    i.remove();
                    break;
                }
            }
        }

        // Throw out all supertypes that are supertypes of some other supertype
        for (Iterator<TypeToken<?>> i = supertypes.iterator(); i.hasNext(); ) {
            final TypeToken<?> supertype = i.next();
            for (TypeToken<?> supertype2 : supertypes) {
                if (supertype2 != supertype && supertype.isAssignableFrom(supertype2)) {
                    i.remove();
                    break;
                }
            }
        }

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

        // There are now only interfaces to choose from (or Object)... try JObject
        final TypeToken<JObject> jobjectType = TypeToken.of(JObject.class);
        if (supertypes.contains(jobjectType))
            return jobjectType;

        // Last resort is Object
        return objectType;
    }

    @SuppressWarnings("unchecked")
    private static <T> void addSupertypes(Set<TypeToken<?>> types, TypeToken<T> type) {
        if (type == null || !types.add(type))
            return;
        final Class<? super T> rawType = type.getRawType();
        types.add(TypeToken.of(rawType));
        types.add(Util.getWildcardedType(rawType));
        final Class<? super T> superclass = rawType.getSuperclass();
        if (superclass != null) {
            Util.addSupertypes(types, TypeToken.of(superclass));
            Util.addSupertypes(types, type.getSupertype(superclass));
        }
        for (Class<?> iface : rawType.getInterfaces())
            Util.addSupertypes(types, type.getSupertype((Class<? super T>)iface));
    }

    /**
     * Parameterize the raw type with wildcards.
     */
    public static <T> TypeToken<? extends T> getWildcardedType(Class<T> type) {
        if (type == null)
            throw new IllegalArgumentException("null type");
        final TypeVariable<Class<T>>[] typeVariables = type.getTypeParameters();
        if (typeVariables.length == 0)
            return TypeToken.of(type);
        final WildcardType[] questionMarks = new WildcardType[typeVariables.length];
        Arrays.fill(questionMarks, QUESTION_MARK);
        return Util.newParameterizedType(type, questionMarks);
    }

    /**
     * Get the n'th generic type parameter.
     *
     * @throws IllegalArgumentException if {@code type} is not a parameterized type with more than {@code index} type variables
     */
    public static Type getTypeParameter(Type type, int index) {
        if (!(type instanceof ParameterizedType))
            throw new IllegalArgumentException("type is missing generic type parameter(s)");
        final ParameterizedType parameterizedType = (ParameterizedType)type;
        final Type[] parameters = parameterizedType.getActualTypeArguments();
        if (index >= parameters.length)
            throw new IllegalArgumentException("type is missing generic type parameter(s)");
        return parameters[index];
    }

    /**
     * Invoke method via reflection and re-throw any checked exception wrapped in an {@link JSimpleDBException}.
     */
    public static Object invoke(Method method, Object target, Object... params) {
        try {
            return method.invoke(target, params);
        } catch (InvocationTargetException e) {
            if (e.getCause() instanceof Error)
                throw (Error)e.getCause();
            if (e.getCause() instanceof RuntimeException)
                throw (RuntimeException)e.getCause();
            throw new JSimpleDBException("unexpected error invoking method " + method + " on " + target, e);
        } catch (Error e) {
            throw e;
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new JSimpleDBException("unexpected error invoking method " + method + " on " + target, e);
        }
    }

    /**
     * Convert a raw class back into its generic type using caller-supplied type parameters.
     *
     * @param target raw class
     * @param params type parameters
     * @return generic {@link TypeToken} for {@code target}
     * @see <a href="https://code.google.com/p/guava-libraries/issues/detail?id=1645">Guava Issue #1645</a>
     */
    @SuppressWarnings("unchecked")
    private static <T> TypeToken<? extends T> newParameterizedType(Class<T> target, Type[] params) {
        Type type;
        try {
            if (Util.newParameterizedTypeMethod == null) {
                Util.newParameterizedTypeMethod = Class.forName("com.google.common.reflect.Types",
                  false, Thread.currentThread().getContextClassLoader()).getDeclaredMethod(
                  "newParameterizedType", Class.class, Type[].class);
                Util.newParameterizedTypeMethod.setAccessible(true);
            }
            type = (Type)Util.newParameterizedTypeMethod.invoke(null, target, params);
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new RuntimeException("unexpected exception", e);
        }
        return (TypeToken<T>)TypeToken.of(type);
    }
}

