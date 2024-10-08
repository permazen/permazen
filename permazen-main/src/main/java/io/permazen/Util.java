
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen;

import com.google.common.base.Converter;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.reflect.TypeToken;

import io.permazen.annotation.OnValidate;
import io.permazen.util.ApplicationClassLoader;

import jakarta.validation.Constraint;
import jakarta.validation.groups.Default;

import java.lang.annotation.Annotation;
import java.lang.reflect.AnnotatedElement;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Various utility routines.
 */
public final class Util {

    private static final String ANNOTATION_ELEMENT_UTILS_CLASS_NAME = "org.springframework.core.annotation.AnnotatedElementUtils";
    private static final String ANNOTATION_ELEMENT_UTILS_GET_MERGED_ANNOTATION_METHOD_NAME = "getMergedAnnotation";

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
                    final Class<?> cl = Class.forName(
                      ANNOTATION_ELEMENT_UTILS_CLASS_NAME, true, ApplicationClassLoader.getInstance());
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
        final HashSet<Class<?>> visited = new HashSet<>();
        visited.add(type);
        for (TypeToken<?> typeToken : TypeToken.of(type).getTypes()) {
            final Class<?> superType = typeToken.getRawType();
            if (!visited.add(superType))
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
     * Identify the named field in the given {@link PermazenClass}.
     *
     * <p>
     * The field may be specified by name like {@code "myfield"} or by name and storage ID like {@code "myfield#1234"}.
     *
     * <p>
     * To specify a sub-field of a complex field, qualify it with the parent field like {@code "mymap.key"}.
     *
     * <p>
     * This method is equivalent to {@code findField(pclass, fieldName, null)}.
     *
     * @param pclass containing object type
     * @param fieldName field name
     * @return resulting {@link PermazenField}, or null if no such field is found in {@code pclass}
     * @throws IllegalArgumentException if {@code fieldName} is ambiguous or invalid
     * @throws IllegalArgumentException if {@code pclass} or {@code fieldName} is null
     */
    public static PermazenField findField(PermazenClass<?> pclass, String fieldName) {
        return Util.findField(pclass, fieldName, null);
    }

    /**
     * Identify the named simple field in the given {@link PermazenClass}.
     *
     * <p>
     * The field may be specified by name like {@code "myfield"} or by name and storage ID like {@code "myfield#1234"}.
     *
     * <p>
     * To specify a sub-field of a complex field, qualify it with the parent field like {@code "mymap.key"}.
     *
     * <p>
     * This method is equivalent to {@code findField(pclass, fieldName, true)} plus filtering out non-{@link PermazenSimpleField}s.
     *
     * @param pclass containing object type
     * @param fieldName field name
     * @return resulting {@link PermazenField}, or null if no such field is found in {@code pclass}
     * @throws IllegalArgumentException if {@code fieldName} is ambiguous or invalid
     * @throws IllegalArgumentException if {@code pclass} or {@code fieldName} is null
     */
    public static PermazenSimpleField findSimpleField(PermazenClass<?> pclass, String fieldName) {
        final PermazenField pfield = Util.findField(pclass, fieldName, true);
        try {
            return (PermazenSimpleField)pfield;
        } catch (ClassCastException e) {
            return null;
        }
    }

    /**
     * Identify the named field in the given {@link PermazenClass}.
     *
     * <p>
     * The field may be specified by name like {@code "myfield"} or by name and storage ID like {@code "myfield#1234"}.
     *
     * <p>
     * To specify a sub-field of a complex field, qualify it with the parent field like {@code "mymap.key"}.
     *
     * <p>
     * The {@code expectSubField} parameter controls what happens when a complex field is matched. If true, then
     * either a sub-field must be specified, or else the complex field must have only one sub-field and then that
     * sub-field is assumed. If false, it is an error to specify a sub-field of a complex field. If null, either is OK.
     *
     * @param pclass containing object type
     * @param fieldName field name
     * @param expectSubField true if the field should be a complex sub-field instead of a complex field,
     *  false if field should not be complex field instead of a complex sub-field, or null for don't care
     * @return resulting {@link PermazenField}, or null if no such field is found in {@code pclass}
     * @throws IllegalArgumentException if {@code fieldName} is ambiguous or invalid
     * @throws IllegalArgumentException if {@code pclass} or {@code fieldName} is null
     */
    public static PermazenField findField(PermazenClass<?> pclass, final String fieldName, Boolean expectSubField) {

        // Sanity check
        Preconditions.checkArgument(pclass != null, "null pclass");
        Preconditions.checkArgument(fieldName != null, "null fieldName");

        // Logging
        final Logger log = LoggerFactory.getLogger(Util.class);
        if (log.isTraceEnabled())
            log.trace("Util.findField(): pclass={} fieldName={} expectSubField={}", pclass, fieldName, expectSubField);

        // Split field name into components
        final ArrayDeque<String> components = new ArrayDeque<>(Arrays.asList(fieldName.split("\\.", -1)));
        if (components.isEmpty() || components.size() > 2)
            throw new IllegalArgumentException(String.format("invalid field name \"%s\"", fieldName));

        // Represents a parsed field name with optional storage ID
        class NameParse {

            final String name;
            final int storageId;

            NameParse(String name) {
                final int hash = name.indexOf('#');
                if (hash == -1) {
                    this.name = name;
                    this.storageId = 0;
                } else {
                    try {
                        if ((this.storageId = Integer.parseInt(name.substring(hash + 1))) <= 0)
                            throw new NumberFormatException();
                    } catch (NumberFormatException e) {
                        throw new IllegalArgumentException(String.format("invalid field name \"%s\"", name));
                    }
                    this.name = name.substring(0, hash);
                }
            }

            boolean matches(PermazenField pfield) {
                return pfield.name.equals(this.name) && (this.storageId == 0 || pfield.storageId == this.storageId);
            }

            @Override
            public String toString() {
                return this.name;
            }
        }

        // Get first field name component
        final NameParse firstName = new NameParse(components.removeFirst());

        // Find the PermazenField matching 'component' in pclass
        PermazenField matchingField = pclass.fieldsByName.get(firstName.name);
        if (matchingField == null || !firstName.matches(matchingField))
            return null;

        // Logging
        if (log.isTraceEnabled())
            log.trace("Util.findField(): found field {} in {}", matchingField, pclass.getType());

        // Get sub-field requirements
        final boolean requireSimpleField = Boolean.TRUE.equals(expectSubField);
        final boolean disallowSubField = Boolean.FALSE.equals(expectSubField);

        // Handle complex fields
        if (matchingField instanceof PermazenComplexField) {

            // Get complex field
            final PermazenComplexField complexField = (PermazenComplexField)matchingField;
            String description = "field \"" + firstName + "\" in " + pclass;

            // Logging
            if (log.isTraceEnabled())
                log.trace("Util.findField(): field is a complex field");

            // If no sub-field is given, field has only one sub-field, and a simple field is required, then default to that
            if (requireSimpleField && components.isEmpty()) {
                final List<PermazenSimpleField> subFields = complexField.getSubFields();
                if (subFields.size() == 1)
                    components.add(subFields.get(0).name);
            }

            // Is there a sub-field component?
            if (!components.isEmpty()) {

                // Find the specified sub-field
                final NameParse subFieldName = new NameParse(components.removeFirst());
                description = String.format("sub-field \"%s\" of %s", subFieldName.name, description);
                try {
                    matchingField = complexField.getSubField(subFieldName.name);
                    if (!subFieldName.matches(matchingField)) {
                        throw new IllegalArgumentException(String.format("explicit storage ID %d != field storage ID %d",
                          subFieldName.storageId, matchingField.storageId));
                    }
                } catch (IllegalArgumentException e) {
                    throw new IllegalArgumentException(String.format("invalid %s: %s", description, e.getMessage()), e);
                }

                // Verify it's OK to specify a complex sub-field
                if (disallowSubField) {
                    throw new IllegalArgumentException(String.format(
                      "invalid %s: %s", description, "instead, specify the complex field itself"));
                }

                // Logging
                if (log.isTraceEnabled()) {
                    log.trace("Util.findField(): also stepping through sub-field [{}.{}] to reach {}",
                      firstName, subFieldName, matchingField);
                }
            } else {

                // Verify it's OK to end on a complex field
                if (requireSimpleField) {
                    final String hints = complexField.getSubFields().stream()
                      .map(subField -> String.format("\"%s\"", subField.getFullName()))
                      .collect(Collectors.joining(" or "));
                    throw new IllegalArgumentException(String.format(
                      "for complex %s a sub-field must be specified (i.e., %s)", description, hints));
                }

                // Done
                if (log.isTraceEnabled())
                    log.trace("Util.findField(): ended on complex field; result={}", matchingField);
            }
        } else if (log.isTraceEnabled()) {
            if (matchingField instanceof PermazenSimpleField) {
                final PermazenSimpleField simpleField = (PermazenSimpleField)matchingField;
                log.trace("Util.findField(): field is a simple field of type {}", simpleField.getTypeToken());
            } else
                log.trace("Util.findField(): field is {}", matchingField);
        }

        // Check for extra garbage
        if (!components.isEmpty())
            throw new IllegalArgumentException(String.format("invalid field name \"%s\"", fieldName));

        // Done
        if (log.isTraceEnabled())
            log.trace("Util.findField(): result={}", matchingField);
        return matchingField;
    }

    /**
     * Find the getter method we should override corresponding to the nominal getter method.
     *
     * <p>
     * This deals with generic sub-type bridge methods.
     *
     * @param type Java type (possibly a sub-type of the type in which {@code getter} is declared)
     * @param getter supertype Java bean property getter method
     * @return corresponding Java bean property getter method in {@code type}, possibly {@code getter}
     */
    static <T> Method findPermazenFieldGetterMethod(Class<T> type, Method getter) {
        Preconditions.checkArgument(type != null);
        Preconditions.checkArgument(getter != null);
        Preconditions.checkArgument(getter.getParameterTypes().length == 0);
        Preconditions.checkArgument(getter.getReturnType() != void.class);
        final TypeToken<T> typeType = TypeToken.of(type);
        final TypeToken<?> propertyType = typeType.resolveType(getter.getGenericReturnType());
        for (TypeToken<?> superType : TypeToken.of(type).getTypes()) {
            for (Method method : superType.getRawType().getDeclaredMethods()) {
                if (!method.getName().equals(getter.getName()))
                    continue;
                if (method.getParameterTypes().length != 0)
                    continue;
                if (!typeType.resolveType(method.getGenericReturnType()).equals(propertyType))
                    continue;
                if ((method.getModifiers() & (Modifier.PROTECTED | Modifier.PUBLIC)) == 0
                  || (method.getModifiers() & Modifier.PRIVATE) != 0) {
                    throw new IllegalArgumentException(String.format(
                      "invalid getter method %s(): %s", getter.getName(), "method must be public or protected"));
                }
                return method;
            }
        }
        return getter;
    }

    /**
     * Find the setter method corresponding to a getter method. It must be either public or protected.
     *
     * @param type Java type (possibly a sub-type of the type in which {@code getter} is declared)
     * @param getter Java bean property getter method
     * @return Java bean property setter method
     * @throws IllegalArgumentException if no corresponding setter method exists
     */
    static <T> Method findPermazenFieldSetterMethod(Class<T> type, Method getter) {
        final Matcher matcher = Pattern.compile("(is|get)(.+)").matcher(getter.getName());
        if (!matcher.matches()) {
            throw new IllegalArgumentException(String.format(
              "can't infer setter method name from getter method %s() because name does not follow Java bean naming conventions",
              getter.getName()));
        }
        final String setterName = "set" + matcher.group(2);
        final TypeToken<T> typeType = TypeToken.of(type);
        final TypeToken<?> propertyType = typeType.resolveType(getter.getGenericReturnType());
        for (TypeToken<?> superType : TypeToken.of(type).getTypes()) {
            for (Method setter : superType.getRawType().getDeclaredMethods()) {
                if (!setter.getName().equals(setterName) || setter.getReturnType() != void.class)
                    continue;
                final Type[] ptypes = setter.getGenericParameterTypes();
                if (ptypes.length != 1)
                    continue;
                if (!typeType.resolveType(ptypes[0]).equals(propertyType))
                    continue;
                if ((setter.getModifiers() & (Modifier.PROTECTED | Modifier.PUBLIC)) == 0
                  || (setter.getModifiers() & Modifier.PRIVATE) != 0) {
                    throw new IllegalArgumentException(String.format(
                      "invalid setter method %s() corresponding to getter method %s(): method must be public or protected",
                      setterName, getter.getName()));
                }
                return setter;
            }
        }
        throw new IllegalArgumentException(String.format(
          "can't find any setter method %s() corresponding to getter method %s() taking %s and returning void",
          setterName, getter.getName(), getter.getReturnType()));
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
     * Check whether a generic type and its erased equivalent match the same set of candidate types.
     *
     * @param genType parameter generic type
     * @param candidates possible candidates for parameter
     * @return mismatching candidate, or null if generic and erased match the same candidates
     */
    public static TypeToken<?> findErasureDifference(TypeToken<?> genType, Collection<? extends TypeToken<?>> candidates) {
        Preconditions.checkArgument(genType != null, "null genType");
        Preconditions.checkArgument(candidates != null, "null candidates");
        final Class<?> rawType = genType.getRawType();
        for (TypeToken<?> candidate : candidates) {
            final boolean matchesGen = genType.isSupertypeOf(candidate);
            final boolean matchesRaw = rawType.isAssignableFrom(candidate.getRawType());
            if (matchesGen != matchesRaw)
                return candidate;
        }
        return null;
    }

    /**
     * Substitute for {@link Stream#of(Object)}.
     *
     * <p>
     * Permazen needs this method because the v1.6 class files we generate don't support invoking
     * static methods on interfaces.
     */
    public static <T> Stream<T> streamOf(T obj) {
        return Stream.of(obj);
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
     * @throws PermazenException if an error occurs
     */
    public static Object invoke(Method method, Object target, Object... params) {
        try {
            return method.invoke(target, params);
        } catch (InvocationTargetException e) {
            Throwables.throwIfUnchecked(e.getCause());
            throw new PermazenException(String.format("unexpected error invoking method %s on %s", method, target), e);
        } catch (Exception e) {
            Throwables.throwIfUnchecked(e);
            throw new PermazenException(String.format("unexpected error invoking method %s on %s", method, target), e);
        }
    }

    /**
     * Return the reverse of the given {@link Converter}, treating a null converter as if it were the identity.
     *
     * @param converter original converter, or null for the identity conversion
     * @return reversed converter
     */
    @SuppressWarnings("unchecked")
    public static <A, B> Converter<B, A> reverse(Converter<A, B> converter) {
        if (converter != null)
            return converter.reverse();
        return (Converter<B, A>)Converter.<Object>identity();
    }
}
