
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen;

import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.Iterables;
import com.google.common.reflect.TypeToken;

import io.permazen.annotation.OnValidate;

import java.lang.annotation.Annotation;
import java.lang.reflect.AnnotatedElement;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.lang.reflect.TypeVariable;
import java.lang.reflect.WildcardType;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.validation.Constraint;
import javax.validation.groups.Default;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Various utility routines.
 */
public final class Util {

    private static final String ANNOTATION_ELEMENT_UTILS_CLASS_NAME = "org.springframework.core.annotation.AnnotatedElementUtils";
    private static final String ANNOTATION_ELEMENT_UTILS_GET_MERGED_ANNOTATION_METHOD_NAME = "getMergedAnnotation";

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
    private static BiFunction<AnnotatedElement, Class<? extends Annotation>, Annotation> annotationRetriever;

    private Util() {
    }

    /**
     * Find the annotation on the given element.
     *
     * <p>
     * If {@code spring-core} is available on the classpath, the implementation in {@link AnnotationScanner} utilizes Spring's
     * {@link org.springframework.core.annotation.AnnotatedElementUtils#getMergedAnnotation(AnnotatedElement, Class)} method
     * to find annotations that are either <i>present</i> or <i>meta-present</i> on the element, and includes support for
     * <i>annotation attribute overrides</i>; otherwise, it just invokes {@link AnnotatedElement#getAnnotation(Class)}.
     *
     * @param element element with annotation or meta-present annotation
     * @param annotationType type of the annotation to find
     * @param <A> annotation type
     * @return the annotation found, or null if not found
     */
    public static <A extends Annotation> A getAnnotation(AnnotatedElement element, Class<A> annotationType) {
        Preconditions.checkArgument(element != null, "null element");
        Preconditions.checkArgument(annotationType != null, "null annotationType");
        synchronized (Util.class) {
            if (Util.annotationRetriever == null) {
                final Logger log = LoggerFactory.getLogger(Util.class);
                try {
                    final Class<?> cl = Class.forName(ANNOTATION_ELEMENT_UTILS_CLASS_NAME,
                      true, Thread.currentThread().getContextClassLoader());
                    final Method method = cl.getMethod(ANNOTATION_ELEMENT_UTILS_GET_MERGED_ANNOTATION_METHOD_NAME,
                      AnnotatedElement.class, Class.class);
                    Util.annotationRetriever = (elem, atype) -> {
                        try {
                            return atype.cast(method.invoke(null, elem, atype));
                        } catch (Exception e) {
                            throw new RuntimeException("internal error", e);
                        }
                    };
                    if (log.isDebugEnabled())
                        log.debug("using Spring's " + cl.getSimpleName() + "." + method.getName() + "() for annotation retrieval");
                } catch (ClassNotFoundException e) {
                    if (log.isDebugEnabled())
                        log.debug("using JDK AnnotatedElement.getAnnotation() for annotation retrieval");
                    Util.annotationRetriever = (elem, atype) -> elem.getAnnotation(atype);
                } catch (Exception e) {
                    log.warn("using JDK AnnotatedElement.getAnnotation() for annotation retrieval", e);
                    Util.annotationRetriever = (elem, atype) -> elem.getAnnotation(atype);
                }
            }
            assert Util.annotationRetriever != null;
        }
        return annotationType.cast(Util.annotationRetriever.apply(element, annotationType));
    }

    /**
     * Determine if any JSR 303 validation annotations are present on the given type itself
     * or any of its methods (<i>public</i> methods only).
     *
     * @param type object type
     * @return a non-null object with JSR 303 validation requirements, or null if none found
     * @throws IllegalArgumentException if {@code type} is null
     */
    public static AnnotatedElement hasValidation(Class<?> type) {

        // Sanity check
        Preconditions.checkArgument(type != null, "null type");

        // Check for annotations on the class itself
        if (Util.hasValidationAnnotation(type))
            return type;

        // Check methods
        for (Method method : type.getDeclaredMethods()) {

            // Check for JSR 303 annotation
            if ((method.getModifiers() & Modifier.PUBLIC) != 0 && Util.hasValidationAnnotation(method))
                return method;
        }

        // Recurse on supertypes
        for (TypeToken<?> typeToken : TypeToken.of(type).getTypes()) {
            final Class<?> superType = typeToken.getRawType();
            if (superType == type)
                continue;
            final AnnotatedElement annotatedElement = Util.hasValidation(superType);
            if (annotatedElement != null)
                return annotatedElement;
        }

        // None found
        return null;
    }

    /**
     * Determine if instances of the given type require any validation under the default validation group.
     *
     * <p>
     * This will be true if {@code type} or any of its declared methods has a JSR 303 (<i>public</i> methods only)
     * or {@link OnValidate &#64;OnValidate} annotation, or if any of its super-types requires validation.
     *
     * @param type object type
     * @return true if {@code type} has any validation requirements
     * @throws IllegalArgumentException if {@code type} is null
     * @see ValidationMode
     */
    public static boolean requiresDefaultValidation(Class<?> type) {

        // Sanity check
        Preconditions.checkArgument(type != null, "null type");

        // Check for annotations on the class itself
        if (Util.hasDefaultValidationAnnotation(type))
            return true;

        // Check methods
        for (Method method : type.getDeclaredMethods()) {

            // Check for @OnValidate annotation
            if (method.isAnnotationPresent(OnValidate.class))
                return true;

            // Check for JSR 303 annotation
            if ((method.getModifiers() & Modifier.PUBLIC) != 0 && Util.requiresDefaultValidation(method))
                return true;
        }

        // Recurse on superclasses
        for (TypeToken<?> typeToken : TypeToken.of(type).getTypes()) {
            final Class<?> superType = typeToken.getRawType();
            if (superType != type && Util.requiresDefaultValidation(superType))
                return true;
        }

        // Done
        return false;
    }

    /**
     * Determine if the given getter method, or any method it overrides, has a JSR 303 validation constraint
     * applicable under the default validation group.
     *
     * @param method annotated method
     * @return true if {@code obj} has one or more JSR 303 annotations
     * @throws IllegalArgumentException if {@code method} is null
     */
    public static boolean requiresDefaultValidation(Method method) {
        Preconditions.checkArgument(method != null, "null method");
        final String methodName = method.getName();
        final Class<?>[] paramTypes = method.getParameterTypes();
        for (TypeToken<?> typeToken : TypeToken.of(method.getDeclaringClass()).getTypes()) {
            final Class<?> superType = typeToken.getRawType();
            try {
                method = superType.getMethod(methodName, paramTypes);
            } catch (NoSuchMethodException e) {
                continue;
            }
            if (Util.hasDefaultValidationAnnotation(method))
                return true;
        }
        return false;
    }

    /**
     * Determine whether the given object has any JSR 303 annotation(s) defining validation constraints in the default group.
     *
     * @param obj annotated element
     * @return true if {@code obj} has one or more JSR 303 default validation constraint annotations
     * @throws IllegalArgumentException if {@code obj} is null
     */
    public static boolean hasDefaultValidationAnnotation(AnnotatedElement obj) {
        return Util.hasValidationAnnotation(obj, new Class<?>[] { Default.class });
    }

    /**
     * Determine whether the given object has any JSR 303 annotation(s).
     *
     * @param obj annotated element
     * @return true if {@code obj} has one or more JSR 303 validation constraint annotations
     * @throws IllegalArgumentException if {@code obj} is null
     */
    public static boolean hasValidationAnnotation(AnnotatedElement obj) {
        return Util.hasValidationAnnotation(obj, null);
    }

    private static boolean hasValidationAnnotation(AnnotatedElement obj, Class<?>[] validationGroups) {
        Preconditions.checkArgument(obj != null, "null obj");
        for (Annotation annotation : obj.getAnnotations()) {
            final Class<?> annotationType = annotation.annotationType();
            if (!annotationType.isAnnotationPresent(Constraint.class))
                continue;
            final Class<?>[] groups;
            try {
                groups = (Class<?>[])annotation.annotationType().getMethod("groups").invoke(annotation);
            } catch (NoSuchMethodException e) {
                return true;
            } catch (RuntimeException e) {
                throw e;
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            if (groups == null || groups.length == 0)
                return true;
            return validationGroups == null || Util.isAnyGroupBeingValidated(groups, validationGroups);
        }
        return false;
    }

    /**
     * Determine if a constraint whose {@code groups()} contain the given constraint group should be applied
     * when validating with the given validation groups.
     *
     * @param constraintGroup validation group associated with a validation constraint
     * @param validationGroups groups for which validation is being performed
     * @return whether to apply the validation constraint
     * @throws IllegalArgumentException if any null values are encountered
     */
    public static boolean isGroupBeingValidated(Class<?> constraintGroup, Class<?>[] validationGroups) {
        Preconditions.checkArgument(constraintGroup != null, "null constraintGroup");
        Preconditions.checkArgument(validationGroups != null, "null validationGroups");
        for (Class<?> validationGroup : validationGroups) {
            Preconditions.checkArgument(validationGroup != null, "null validationGroup");
            if (constraintGroup.isAssignableFrom(validationGroup))
                return true;
        }
        return false;
    }

    /**
     * Determine if a constraint whose {@code groups()} contain the given constraint groups should be applied
     * when validating with the given validation groups.
     *
     * @param constraintGroups validation groups associated with a validation constraint
     * @param validationGroups groups for which validation is being performed
     * @return whether to apply the validation constraint
     * @throws IllegalArgumentException if any null values are encountered
     */
    public static boolean isAnyGroupBeingValidated(Class<?>[] constraintGroups, Class<?>[] validationGroups) {
        Preconditions.checkArgument(constraintGroups != null, "null constraintGroups");
        for (Class<?> constraintGroup : constraintGroups) {
            if (Util.isGroupBeingValidated(constraintGroup, validationGroups))
                return true;
        }
        return false;
    }

    /**
     * Find the setter method corresponding to a getter method. It must be either public or protected.
     *
     * @param type Java type (possibly a sub-type of the type in which {@code getter} is declared)
     * @param getter Java bean property getter method
     * @return Java bean property setter method
     * @throws IllegalArgumentException if no corresponding setter method exists
     */
    static <T> Method findJFieldSetterMethod(Class<T> type, Method getter) {
        final Matcher matcher = Pattern.compile("(is|get)(.+)").matcher(getter.getName());
        if (!matcher.matches()) {
            throw new IllegalArgumentException("can't infer setter method name from getter method "
              + getter.getName() + "() because name does not follow Java bean naming conventions");
        }
        final String setterName = "set" + matcher.group(2);
        final TypeToken<T> typeType = TypeToken.of(type);
        final TypeToken<?> propertyType = typeType.resolveType(getter.getGenericReturnType());
        for (TypeToken<?> superType : TypeToken.of(type).getTypes()) {
            for (Method setter : superType.getRawType().getDeclaredMethods()) {
                if (!setter.getName().equals(setterName) || setter.getReturnType() != Void.TYPE)
                    continue;
                final Type[] ptypes = setter.getGenericParameterTypes();
                if (ptypes.length != 1)
                    continue;
                if (!typeType.resolveType(ptypes[0]).equals(propertyType))
                    continue;
                if ((setter.getModifiers() & (Modifier.PROTECTED | Modifier.PUBLIC)) == 0
                  || (setter.getModifiers() & Modifier.PRIVATE) != 0) {
                    throw new IllegalArgumentException("invalid setter method " + setterName
                      + "() corresponding to getter method " + getter.getName() + "(): method must be public or protected");
                }
                return setter;
            }
        }
        throw new IllegalArgumentException("can't find any setter method " + setterName
          + "() corresponding to getter method " + getter.getName() + "() taking " + getter.getReturnType()
          + " and returning void");
    }

    /**
     * Find unimplemented abstract methods in the given class.
     */
    static Map<MethodKey, Method> findAbstractMethods(Class<?> type) {
        final HashMap<MethodKey, Method> map = new HashMap<>();

        // First find all methods, but don't include overridden supertype methods
        for (TypeToken<?> superType : TypeToken.of(type).getTypes()) {
            for (Method method : superType.getRawType().getDeclaredMethods()) {
                final MethodKey key = new MethodKey(method);
                if (!map.containsKey(key))
                    map.put(key, method);
            }
        }

        // Now discard all the non-abstract methods
        for (Iterator<Map.Entry<MethodKey, Method>> i = map.entrySet().iterator(); i.hasNext(); ) {
            final Map.Entry<MethodKey, Method> entry = i.next();
            if ((entry.getValue().getModifiers() & Modifier.ABSTRACT) == 0)
                i.remove();
        }

        // Done
        return map;
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
     * @throws IllegalArgumentException if any type in {@code types} is null
     * @see #findLowestCommonAncestor findLowestCommonAncestor()
     */
    public static TypeToken<?> findLowestCommonAncestorOfClasses(Iterable<Class<?>> types) {
        types.forEach(type -> {
            if (type == null)
                throw new IllegalArgumentException("null type");
        });
        return Util.findLowestCommonAncestor(Iterables.transform(types, TypeToken::of));
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
    public static Set<TypeToken<?>> findLowestCommonAncestorsOfClasses(Iterable<Class<?>> types) {
        types.forEach(type -> {
            if (type == null)
                throw new IllegalArgumentException("null type");
        });
        return Util.findLowestCommonAncestors(Iterables.transform(types, TypeToken::of));
    }

    /**
     * Find the narrowest type that is a supertype of all of the given types.
     *
     * <p>
     * Note that there may be more than one such type. The returned type will always be as narrow
     * as possible, but it's possible there for there to be multiple such types for which none
     * is a sub-type of any other.
     *
     * <p>
     * When there is more than one choice, heuristics are used. For example, we prefer
     * non-interface types, and {@link JObject} over other interface types.
     *
     * @param types sub-types
     * @return narrowest common super-type
     */
    public static TypeToken<?> findLowestCommonAncestor(Iterable<TypeToken<?>> types) {

        // Gather candidates
        final Set<TypeToken<?>> supertypes = Util.findLowestCommonAncestors(types);

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
    public static Set<TypeToken<?>> findLowestCommonAncestors(Iterable<TypeToken<?>> types) {

        // Gather all supertypes of types recursively
        final HashSet<TypeToken<?>> supertypes = new HashSet<>();
        for (TypeToken<?> type : types)
            Util.addSupertypes(supertypes, type);

        // Throw out all supertypes that are not supertypes of every type
        for (Iterator<TypeToken<?>> i = supertypes.iterator(); i.hasNext(); ) {
            final TypeToken<?> supertype = i.next();
            for (TypeToken<?> type : types) {
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
        return Util.newParameterizedType(type, questionMarks);
    }

    /**
     * Get the n'th generic type parameter.
     *
     * @param type parameterized generic type
     * @param index type parameter index (zero based)
     * @return type parameter at {@code index}
     * @throws IllegalArgumentException if {@code type} is not a parameterized type with more than {@code index} type variables
     */
    public static Type getTypeParameter(Type type, int index) {
        Preconditions.checkArgument(type instanceof ParameterizedType, "type is missing generic type parameter(s)");
        final ParameterizedType parameterizedType = (ParameterizedType)type;
        final Type[] parameters = parameterizedType.getActualTypeArguments();
        if (index >= parameters.length)
            throw new IllegalArgumentException("type is missing generic type parameter(s)");
        return parameters[index];
    }

    /**
     * Invoke method via reflection and re-throw any checked exception wrapped in an {@link PermazenException}.
     *
     * @param method method to invoke
     * @param target instance, or null if method is static
     * @param params method parameters
     * @return method return value
     */
    public static Object invoke(Method method, Object target, Object... params) {
        try {
            return method.invoke(target, params);
        } catch (InvocationTargetException e) {
            Throwables.throwIfUnchecked(e.getCause());
            throw new PermazenException("unexpected error invoking method " + method + " on " + target, e);
        } catch (Exception e) {
            Throwables.throwIfUnchecked(e);
            throw new PermazenException("unexpected error invoking method " + method + " on " + target, e);
        }
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

