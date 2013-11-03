
/*
 * Copyright (C) 2011 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.dellroad.stuff.vaadin7;

import com.vaadin.data.Validator;
import com.vaadin.data.fieldgroup.BeanFieldGroup;
import com.vaadin.data.util.BeanItem;
import com.vaadin.data.util.converter.Converter;
import com.vaadin.shared.ui.combobox.FilteringMode;
import com.vaadin.shared.ui.datefield.Resolution;
import com.vaadin.ui.AbstractField;
import com.vaadin.ui.AbstractSelect;
import com.vaadin.ui.AbstractTextField;
import com.vaadin.ui.CheckBox;
import com.vaadin.ui.ComboBox;
import com.vaadin.ui.DateField;
import com.vaadin.ui.ListSelect;
import com.vaadin.ui.PasswordField;
import com.vaadin.ui.TextArea;
import com.vaadin.ui.TextField;

import java.beans.BeanInfo;
import java.beans.IntrospectionException;
import java.beans.Introspector;
import java.beans.PropertyDescriptor;
import java.lang.annotation.Annotation;
import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.lang.reflect.AccessibleObject;
import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;

/**
 * Automatically builds and binds fields for a Java bean annotated with {@link FieldBuilder} annotations.
 * The various nested {@link FieldBuilder} annotation types annotate Java bean property "getter" methods and specify
 * how the the bean properties of that class should be edited using {@link AbstractField}s. This allows all information
 * about how to edit a Java model class to stay contained within that class.
 *
 * <p>
 * This class supports two types of annotations: first,the {@link ProvidesField} annotation annotates a method that knows
 * how to build an {@link AbstractField} suitable for editing the bean property specified by
 * its {@link ProvidesField#value value()}. So {@link ProvidesField} is analgous to {@link ProvidesProperty}, except that
 * it defines an editing field rather than a container property.
 * </p>
 *
 * <p>
 * The {@link FieldBuilder.AbstractField} hierarchy annotations are the other type of annotation. These annotations annotate
 * a Java bean property getter method and specify how to configure an {@link AbstractField} to edit the bean property
 * corresponding to the getter method.
 * {@link FieldBuilder.AbstractField} is the top level annotation in a hierarchy of annotations that correspond to the
 * {@link AbstractField} class hierarchy. {@link FieldBuilder.AbstractField} corresponds to {@link AbstractField},
 * and its properties configure corresponding {@link AbstractField} properties.
 * More specific annotations correspond to the various {@link AbstractField} subclasses,
 * for example {@link ComboBox FieldBuilder.ComboBox} corresponds to {@link ComboBox}.
 * When using more specific annotations, the "superclass" annotations configure the superclass properties.
 * </p>
 *
 * <p>
 * A simple example shows how these annotations are used:
 * <blockquote><pre>
 * // Use a 10x40 TextArea to edit the "description" property
 * <b>&#64;FieldBuilder.TextArea(columns = 40, rows = 10)</b>
 * <b>&#64;FieldBuilder.AbstractField(caption = "Description:")</b>
 * public String getDescription() {
 *     return this.description;
 * }
 *
 * // Use my own custom field to edit the "foobar" property
 * <b>&#64;FieldBuilder.ProvidesField("foobar")</b>
 * private MyCustomField createFoobarField() {
 *     ...
 * }
 * </pre></blockquote>
 * </p>
 *
 * <p>
 * A {@link FieldBuilder} instance will read these annotations and build the fields automatically. For example:
 * <blockquote><pre>
 * // Create fields based on FieldGroup.* annotations
 * Person person = new Person("Joe Smith", 100);
 * BeanFieldGroup&lt;Person&gt; fieldGroup = <b>FieldBuilder.buildFieldGroup(person)</b>;
 *
 * // Layout the fields in a form
 * FormLayout layout = new FormLayout();
 * for (Field&lt;?&gt; field : fieldGroup.getFields())
 *     layout.addComponent(field);
 * </pre></blockquote>
 * </p>
 *
 * <p>
 * For all annotations in the {@link FieldBuilder.AbstractField} hierarchy, leaving properties set to their default values
 * results in the default behavior.
 * </p>
 *
 * @see AbstractSelect
 * @see AbstractTextField
 * @see CheckBox
 * @see ComboBox
 * @see DateField
 * @see ListSelect
 * @see PasswordField
 * @see TextArea
 * @see TextField
 */
public class FieldBuilder {

    /**
     * Introspect for {@link FieldBuilder} annotations on property getter methods of the
     * {@link BeanFieldGroup}'s data source, and then build and bind the corresponding fields.
     *
     * @param fieldGroup field group to configure
     * @throws IllegalArgumentException if {@code fieldGroup} is null
     * @throws IllegalArgumentException if {@code fieldGroup} does not yet
     *  {@linkplain BeanFieldGroup#setItemDataSource(Object) have a data source}
     */
    public void buildAndBind(BeanFieldGroup<?> fieldGroup) {

        // Sanity check
        if (fieldGroup == null)
            throw new IllegalArgumentException("null beanType");
        final BeanItem<?> beanItem = fieldGroup.getItemDataSource();
        if (beanItem == null)
            throw new IllegalArgumentException("fieldGroup does not yet have a data source");

        // Scan bean properties to build fields
        for (Map.Entry<String, com.vaadin.ui.AbstractField<?>> entry : this.buildBeanPropertyFields(beanItem.getBean()).entrySet())
            fieldGroup.bind(entry.getValue(), entry.getKey());
    }

    /**
     * Introspect for {@link FieldBuilder} annotations on property getter methods and build
     * a mapping from Java bean property name to a field that may be used to edit that property.
     *
     * @param bean Java bean
     * @return mapping from bean property name to field
     * @throws IllegalArgumentException if {@code bean} is null
     * @throws IllegalArgumentException if invalid or conflicting annotations are encountered
     */
    public Map<String, com.vaadin.ui.AbstractField<?>> buildBeanPropertyFields(Object bean) {

        // Sanity check
        if (bean == null)
            throw new IllegalArgumentException("null bean");

        // Look for all bean property getter methods
        BeanInfo beanInfo;
        try {
            beanInfo = Introspector.getBeanInfo(bean.getClass());
        } catch (IntrospectionException e) {
            throw new RuntimeException("unexpected exception", e);
        }
        final ArrayList<Method> getterList = new ArrayList<Method>();
        final HashMap<String, Method> getterMap = new HashMap<String, Method>();
        for (PropertyDescriptor propertyDescriptor : beanInfo.getPropertyDescriptors()) {
            final Method method = propertyDescriptor.getReadMethod();
            if (method != null && method.getReturnType() != void.class && method.getParameterTypes().length == 0)
                getterMap.put(propertyDescriptor.getName(), method);
        }

        // Scan getters for FieldBuilder.* annotations other than FieldBuidler.ProvidesField
        final HashMap<String, com.vaadin.ui.AbstractField<?>> map = new HashMap<String, com.vaadin.ui.AbstractField<?>>();
        for (Map.Entry<String, Method> entry : getterMap.entrySet()) {
            final String propertyName = entry.getKey();
            final Method method = entry.getValue();

            // Get annotations, if any
            final List<AnnotationApplier<?, ?>> applierList = this.buildApplierList(method);
            if (applierList.isEmpty())
                continue;

            // Build field
            map.put(propertyName, this.buildField(applierList, "method " + method));
        }

        // Scan all methods for @FieldBuidler.ProvidesField annotations
        final HashMap<String, Method> providerMap = new HashMap<String, Method>();
        for (Method method : bean.getClass().getDeclaredMethods()) {
            if (method.getReturnType() == void.class || method.getParameterTypes().length > 0)
                continue;
            this.buildProviderMap(providerMap, bean.getClass(), method.getName());
        }

        // Check for conflicts between @FieldBuidler.ProvidesField and other annotations and add fields to map
        for (Map.Entry<String, Method> entry : providerMap.entrySet()) {
            final String propertyName = entry.getKey();
            final Method method = entry.getValue();

            // Verify field is not already defined
            if (map.containsKey(propertyName)) {
                throw new IllegalArgumentException("conflicting annotations exist for property `" + propertyName + "': annotation @"
                  + ProvidesField.class.getName() + " on method " + method
                  + " cannot be combined with other @FieldBuilder.* annotation types");
            }

            // Invoke method to create field
            com.vaadin.ui.AbstractField<?> field;
            try {
                method.setAccessible(true);
            } catch (Exception e) {
                // ignore
            }
            try {
                field = (com.vaadin.ui.AbstractField)method.invoke(bean);
            } catch (Exception e) {
                throw new RuntimeException("error invoking @" + ProvidesField.class.getName()
                  + " annotation on method " + method, e);
            }

            // Save field
            map.put(propertyName, field);
        }

        // Done
        return map;
    }

    // Used by buildBeanPropertyFields()
    private void buildProviderMap(Map<String, Method> providerMap, Class<?> type, String methodName) {

        // Terminate recursion
        if (type == null)
            return;

        // Check the method in this class
        do {

            // Get method
            Method method;
            try {
                method = type.getDeclaredMethod(methodName);
            } catch (NoSuchMethodException e) {
                break;
            }

            // Get annotation
            final ProvidesField providesField = method.getAnnotation(ProvidesField.class);
            if (providesField == null)
                break;

            // Validate method return type is compatible with AbstractField
            if (!com.vaadin.ui.AbstractField.class.isAssignableFrom(method.getReturnType())) {
                throw new IllegalArgumentException("invalid @" + ProvidesField.class.getName() + " annotation on method " + method
                  + ": return type " + method.getReturnType().getName() + " is not a subtype of "
                  + com.vaadin.ui.AbstractField.class.getName());
            }

            // Check for two methods declaring fields for the same property
            final String propertyName = providesField.value();
            final Method otherMethod = providerMap.get(propertyName);
            if (otherMethod != null && !otherMethod.getName().equals(methodName)) {
                throw new IllegalArgumentException("conflicting @" + ProvidesField.class.getName()
                  + " annotations exist for property `" + propertyName + "': both method "
                  + otherMethod + " and method " + method + " are specified");
            }

            // Save method
            if (otherMethod == null)
                providerMap.put(propertyName, method);
        } while (false);

        // Recurse on interfaces
        for (Class<?> iface : type.getInterfaces())
            this.buildProviderMap(providerMap, iface, methodName);

        // Recurse on superclass
        this.buildProviderMap(providerMap, type.getSuperclass(), methodName);
    }

    /**
     * Create a {@link BeanFieldGroup} using the given instance, introspect for {@link FieldBuilder} annotations
     * on property getter methods of the given bean's class, and build and bind the corresponding fields, and return
     * the result.
     *
     * @param bean model bean annotated with {@link FieldBuilder} annotations
     * @throws IllegalArgumentException if {@code bean} is null
     */
    @SuppressWarnings("unchecked")
    public static <T> BeanFieldGroup<T> buildFieldGroup(T bean) {

        // Sanity check
        if (bean == null)
            throw new IllegalArgumentException("null bean");

        // Create field group
        final BeanFieldGroup<T> fieldGroup = new BeanFieldGroup<T>((Class<T>)bean.getClass());
        fieldGroup.setItemDataSource(bean);
        new FieldBuilder().buildAndBind(fieldGroup);
        return fieldGroup;
    }

    /**
     * Instantiate and configure an {@link AbstractField} according to the given scanned annotations.
     */
    protected com.vaadin.ui.AbstractField<?> buildField(Collection<AnnotationApplier<?, ?>> appliers, String description) {

        // Get comparator that sorts by class hierarcy, narrower types first; note Collections.sort() is stable,
        // so for any specific annotation type, that annotation on subtype appears before that annotation on supertype.
        final Comparator<AnnotationApplier<?, ?>> comparator = new Comparator<AnnotationApplier<?, ?>>() {
            @Override
            public int compare(AnnotationApplier<?, ?> a1, AnnotationApplier<?, ?> a2) {
                final Class<? extends com.vaadin.ui.AbstractField<?>> type1 = a1.getFieldType();
                final Class<? extends com.vaadin.ui.AbstractField<?>> type2 = a2.getFieldType();
                if (type1 == type2)
                    return 0;
                if (type1.isAssignableFrom(type2))
                    return 1;
                if (type2.isAssignableFrom(type1))
                    return -1;
                return 0;
            }
        };

        // Sanity check for duplicates and conflicts
        final ArrayList<AnnotationApplier<?, ?>> applierList = new ArrayList<AnnotationApplier<?, ?>>(appliers);
        Collections.sort(applierList, comparator);
        for (int i = 0; i < applierList.size() - 1; ) {
            final AnnotationApplier<?, ?> a1 = applierList.get(i);
            final AnnotationApplier<?, ?> a2 = applierList.get(i + 1);

            // Let annotations on subclass override annotations on superclass
            if (a1.getAnnotation().annotationType() == a2.getAnnotation().annotationType()) {
                applierList.remove(i + 1);
                continue;
            }

            // Check for conflicting annotation types (e.g., both FieldBuilder.TextField and FieldBuilder.DateField)
            if (comparator.compare(a1, a2) == 0) {
                throw new IllegalArgumentException("conflicting annotations of type "
                  + a1.getAnnotation().annotationType().getName() + " and " + a2.getAnnotation().annotationType().getName()
                  + " for " + description);
            }
            i++;
        }

        // Determine field type
        Class<? extends com.vaadin.ui.AbstractField<?>> type = null;
        AnnotationApplier<?, ?> typeApplier = null;
        for (AnnotationApplier<?, ?> applier : applierList) {

            // Pick up type() if specified
            if (applier.getActualFieldType() == null)
                continue;
            if (type == null) {
                type = applier.getActualFieldType();
                typeApplier = applier;
                continue;
            }

            // Verify the field type specified by a narrower annotation has compatible narrower field type
            if (!applier.getActualFieldType().isAssignableFrom(type)) {
                throw new IllegalArgumentException("conflicting field types specified by annotations of type "
                  + typeApplier.getAnnotation().annotationType().getName() + " (type() = " + type.getName() + ") and "
                  + applier.getAnnotation().annotationType().getName() + " (type() = " + applier.getActualFieldType().getName()
                  + ") for " + description);
            }
        }
        if (type == null)
            throw new IllegalArgumentException("cannot determine field type; no type() specified for " + description);

        // Instantiate field
        final com.vaadin.ui.AbstractField<?> field = FieldBuilder.instantiate(type);

        // Configure the field
        for (AnnotationApplier<?, ?> applier : applierList)
            this.apply(applier, field);

        // Done
        return field;
    }

    // This method exists solely to bind the generic type
    private <F extends com.vaadin.ui.AbstractField<?>> void apply(AnnotationApplier<?, F> applier,
      com.vaadin.ui.AbstractField<?> field) {
        applier.applyTo(applier.getFieldType().cast(field));
    }

    private static <T> T instantiate(Class<T> type) {
        Constructor<T> constructor;
        try {
            constructor = type.getDeclaredConstructor();
        } catch (Exception e) {
            throw new RuntimeException("cannot instantiate " + type + " because no zero-arg constructor could be found", e);
        }
        try {
            constructor.setAccessible(true);
        } catch (Exception e) {
            // ignore
        }
        try {
            return constructor.newInstance();
        } catch (Exception e) {
            throw new RuntimeException("cannot instantiate " + type + " using its zero-arg constructor", e);
        }
    }

    /**
     * Find all relevant annotations on the given method as well as on any supertype methods it overrides.
     * The method must be a getter method taking no arguments. Annotations are ordered so that annotations
     * on a method in type X appear before annotations on an overridden method in type Y, a supertype of X.
     *
     * @throws IllegalArgumentException if {@code method} is null
     * @throws IllegalArgumentException if {@code method} has parameters
     */
    protected List<AnnotationApplier<?, ?>> buildApplierList(Method method) {

        // Sanity check
        if (method == null)
            throw new IllegalArgumentException("null method");
        if (method.getParameterTypes().length > 0)
            throw new IllegalArgumentException("method takes parameters");

        // Recurse
        final ArrayList<AnnotationApplier<?, ?>> list = new ArrayList<AnnotationApplier<?, ?>>();
        this.buildApplierList(method.getDeclaringClass(), method.getName(), list);
        return list;
    }

    private void buildApplierList(Class<?> type, String methodName, List<AnnotationApplier<?, ?>> list) {

        // Terminate recursion
        if (type == null)
            return;

        // Check class
        Method method;
        try {
            method = type.getMethod(methodName);
        } catch (NoSuchMethodException e) {
            method = null;
        }
        if (method != null)
            list.addAll(this.buildApplierList((AccessibleObject)method));

        // Recurse on interfaces
        for (Class<?> iface : type.getInterfaces())
            this.buildApplierList(iface, methodName, list);

        // Recurse on superclass
        this.buildApplierList(type.getSuperclass(), methodName, list);
    }

    /**
     * Find all relevant annotations declared directly on the given {@link AccessibleObject}.
     *
     * @throws IllegalArgumentException if {@code member} is null
     */
    protected List<AnnotationApplier<?, ?>> buildApplierList(AccessibleObject member) {

        // Sanity check
        if (member == null)
            throw new IllegalArgumentException("null member");

        // Build list
        final ArrayList<AnnotationApplier<?, ?>> list = new ArrayList<AnnotationApplier<?, ?>>();
        for (Annotation annotation : member.getDeclaredAnnotations()) {
            final AnnotationApplier<? extends Annotation, ?> applier = this.getAnnotationApplier(annotation);
            if (applier != null)
                list.add(applier);
        }
        return list;
    }

    /**
     * Get the {@link AnnotationApplier} that applies the given annotation.
     *
     * @return corresponding {@link AnnotationApplier}, or null if annotation is unknown
     */
    protected AnnotationApplier<?, ?> getAnnotationApplier(Annotation annotation) {
        if (annotation instanceof FieldBuilder.AbstractField)
            return new AbstractFieldApplier((FieldBuilder.AbstractField)annotation);
        if (annotation instanceof FieldBuilder.AbstractSelect)
            return new AbstractSelectApplier((FieldBuilder.AbstractSelect)annotation);
        if (annotation instanceof FieldBuilder.CheckBox)
            return new CheckBoxApplier((FieldBuilder.CheckBox)annotation);
        if (annotation instanceof FieldBuilder.ComboBox)
            return new ComboBoxApplier((FieldBuilder.ComboBox)annotation);
        if (annotation instanceof FieldBuilder.ListSelect)
            return new ListSelectApplier((FieldBuilder.ListSelect)annotation);
        if (annotation instanceof FieldBuilder.DateField)
            return new DateFieldApplier((FieldBuilder.DateField)annotation);
        if (annotation instanceof FieldBuilder.AbstractTextField)
            return new AbstractTextFieldApplier((FieldBuilder.AbstractTextField)annotation);
        if (annotation instanceof FieldBuilder.TextField)
            return new TextFieldApplier((FieldBuilder.TextField)annotation);
        if (annotation instanceof FieldBuilder.TextArea)
            return new TextAreaApplier((FieldBuilder.TextArea)annotation);
        if (annotation instanceof FieldBuilder.PasswordField)
            return new PasswordFieldApplier((FieldBuilder.PasswordField)annotation);
        return null;
    }

// AnnotationApplier

    /**
     * Class that knows how to apply annotation properties to a corresponding field.
     */
    protected abstract static class AnnotationApplier<A extends Annotation, F extends com.vaadin.ui.AbstractField<?>> {

        protected final A annotation;
        protected final Class<F> fieldType;

        protected AnnotationApplier(A annotation, Class<F> fieldType) {
            if (annotation == null)
                throw new IllegalArgumentException("null annotation");
            if (fieldType == null)
                throw new IllegalArgumentException("null fieldType");
            this.annotation = annotation;
            this.fieldType = fieldType;
        }

        public final A getAnnotation() {
            return this.annotation;
        }

        public final Class<F> getFieldType() {
            return this.fieldType;
        }

        public abstract Class<? extends F> getActualFieldType();

        public abstract void applyTo(F field);
    }

    /**
     * Applies properties from a {@link FieldBuilder.AbstractField} annotation to a {@link com.vaadin.ui.AbstractField}.
     */
    private static class AbstractFieldApplier
      extends AnnotationApplier<FieldBuilder.AbstractField, com.vaadin.ui.AbstractField<?>> {

        @SuppressWarnings("unchecked")
        public AbstractFieldApplier(FieldBuilder.AbstractField annotation) {
            super(annotation, (Class<com.vaadin.ui.AbstractField<?>>)(Object)com.vaadin.ui.AbstractField.class);
        }

        @Override
        @SuppressWarnings("unchecked")
        public Class<? extends com.vaadin.ui.AbstractField<?>> getActualFieldType() {
            return (Class<com.vaadin.ui.AbstractField<?>>)(Object)(this.annotation.type() != com.vaadin.ui.AbstractField.class ?
              this.annotation.type() : null);
        }

        @Override
        @SuppressWarnings("unchecked")
        public void applyTo(com.vaadin.ui.AbstractField<?> field) {
            if (this.annotation.width().length() > 0)
                field.setWidth(this.annotation.width());
            if (this.annotation.height().length() > 0)
                field.setHeight(this.annotation.height());
            if (this.annotation.caption().length() > 0)
                field.setCaption(this.annotation.caption());
            if (this.annotation.description().length() > 0)
                field.setDescription(this.annotation.description());
            field.setEnabled(this.annotation.enabled());
            field.setImmediate(this.annotation.immediate());
            field.setReadOnly(this.annotation.readOnly());
            field.setBuffered(this.annotation.buffered());
            field.setInvalidAllowed(this.annotation.invalidAllowed());
            field.setInvalidCommitted(this.annotation.invalidCommitted());
            field.setValidationVisible(this.annotation.validationVisible());
            field.setRequired(this.annotation.required());
            field.setTabIndex(this.annotation.tabIndex());
            if (this.annotation.converter() != Converter.class)
                field.setConverter(FieldBuilder.instantiate(this.annotation.converter()));
            for (Class<? extends Validator> validatorType : this.annotation.validators())
                field.addValidator(FieldBuilder.instantiate(validatorType));
            for (String styleName : this.annotation.styleNames())
                field.addStyleName(styleName);
            if (this.annotation.conversionError().length() > 0)
                field.setConversionError(this.annotation.conversionError());
            if (this.annotation.requiredError().length() > 0)
                field.setRequiredError(this.annotation.requiredError());
        }
    }

    /**
     * Applies properties from a {@link FieldBuilder.AbstractSelect} annotation to a {@link com.vaadin.ui.AbstractSelect}.
     */
    private static class AbstractSelectApplier
      extends AnnotationApplier<FieldBuilder.AbstractSelect, com.vaadin.ui.AbstractSelect> {

        public AbstractSelectApplier(FieldBuilder.AbstractSelect annotation) {
            super(annotation, com.vaadin.ui.AbstractSelect.class);
        }

        @Override
        public Class<? extends com.vaadin.ui.AbstractSelect> getActualFieldType() {
            return this.annotation.type() != com.vaadin.ui.AbstractSelect.class ? this.annotation.type() : null;
        }

        @Override
        public void applyTo(com.vaadin.ui.AbstractSelect field) {
            field.setItemCaptionMode(this.annotation.itemCaptionMode());
            if (this.annotation.itemCaptionPropertyId().length() > 0)
                field.setItemCaptionPropertyId(this.annotation.itemCaptionPropertyId());
            if (this.annotation.itemIconPropertyId().length() > 0)
                field.setItemIconPropertyId(this.annotation.itemIconPropertyId());
            if (this.annotation.nullSelectionItemId().length() > 0)
                field.setNullSelectionItemId(this.annotation.nullSelectionItemId());
            field.setMultiSelect(this.annotation.multiSelect());
            field.setNewItemsAllowed(this.annotation.newItemsAllowed());
            field.setNullSelectionAllowed(this.annotation.nullSelectionAllowed());
        }
    }

    /**
     * Applies properties from a {@link FieldBuilder.CheckBox} annotation to a {@link com.vaadin.ui.CheckBox}.
     */
    private static class CheckBoxApplier extends AnnotationApplier<FieldBuilder.CheckBox, com.vaadin.ui.CheckBox> {

        public CheckBoxApplier(FieldBuilder.CheckBox annotation) {
            super(annotation, com.vaadin.ui.CheckBox.class);
        }

        @Override
        public Class<? extends com.vaadin.ui.CheckBox> getActualFieldType() {
            return this.annotation.type();
        }

        @Override
        public void applyTo(com.vaadin.ui.CheckBox field) {
        }
    }

    /**
     * Applies properties from a {@link FieldBuilder.ComboBox} annotation to a {@link com.vaadin.ui.ComboBox}.
     */
    private static class ComboBoxApplier extends AnnotationApplier<FieldBuilder.ComboBox, com.vaadin.ui.ComboBox> {

        public ComboBoxApplier(FieldBuilder.ComboBox annotation) {
            super(annotation, com.vaadin.ui.ComboBox.class);
        }

        @Override
        public Class<? extends com.vaadin.ui.ComboBox> getActualFieldType() {
            return this.annotation.type();
        }

        @Override
        public void applyTo(com.vaadin.ui.ComboBox field) {
            if (this.annotation.inputPrompt().length() > 0)
                field.setInputPrompt(this.annotation.inputPrompt());
            if (this.annotation.pageLength() != -1)
                field.setPageLength(this.annotation.pageLength());
            field.setScrollToSelectedItem(this.annotation.scrollToSelectedItem());
            field.setTextInputAllowed(this.annotation.textInputAllowed());
            field.setFilteringMode(this.annotation.filteringMode());
        }
    }

    /**
     * Applies properties from a {@link FieldBuilder.ListSelect} annotation to a {@link com.vaadin.ui.ListSelect}.
     */
    private static class ListSelectApplier extends AnnotationApplier<FieldBuilder.ListSelect, com.vaadin.ui.ListSelect> {

        public ListSelectApplier(FieldBuilder.ListSelect annotation) {
            super(annotation, com.vaadin.ui.ListSelect.class);
        }

        @Override
        public Class<? extends com.vaadin.ui.ListSelect> getActualFieldType() {
            return this.annotation.type();
        }

        @Override
        public void applyTo(com.vaadin.ui.ListSelect field) {
            if (this.annotation.rows() != -1)
                field.setRows(this.annotation.rows());
        }
    }

    /**
     * Applies properties from a {@link FieldBuilder.DateField} annotation to a {@link com.vaadin.ui.DateField}.
     */
    private static class DateFieldApplier extends AnnotationApplier<FieldBuilder.DateField, com.vaadin.ui.DateField> {

        public DateFieldApplier(FieldBuilder.DateField annotation) {
            super(annotation, com.vaadin.ui.DateField.class);
        }

        @Override
        public Class<? extends com.vaadin.ui.DateField> getActualFieldType() {
            return this.annotation.type();
        }

        @Override
        public void applyTo(com.vaadin.ui.DateField field) {
            if (this.annotation.dateFormat().length() > 0)
                field.setDateFormat(this.annotation.dateFormat());
            if (this.annotation.parseErrorMessage().length() > 0)
                field.setParseErrorMessage(this.annotation.parseErrorMessage());
            if (this.annotation.dateOutOfRangeMessage().length() > 0)
                field.setDateOutOfRangeMessage(this.annotation.dateOutOfRangeMessage());
            field.setResolution(this.annotation.resolution());
            field.setShowISOWeekNumbers(this.annotation.showISOWeekNumbers());
            if (this.annotation.timeZone().length() > 0)
                field.setTimeZone(TimeZone.getTimeZone(this.annotation.timeZone()));
            field.setLenient(this.annotation.lenient());
        }
    }

    /**
     * Applies properties from a {@link FieldBuilder.AbstractTextField} annotation to a {@link com.vaadin.ui.AbstractTextField}.
     */
    private static class AbstractTextFieldApplier
      extends AnnotationApplier<FieldBuilder.AbstractTextField, com.vaadin.ui.AbstractTextField> {

        public AbstractTextFieldApplier(FieldBuilder.AbstractTextField annotation) {
            super(annotation, com.vaadin.ui.AbstractTextField.class);
        }

        @Override
        public Class<? extends com.vaadin.ui.AbstractTextField> getActualFieldType() {
            return this.annotation.type();
        }

        @Override
        public void applyTo(com.vaadin.ui.AbstractTextField field) {
            field.setNullRepresentation(this.annotation.nullRepresentation());
            field.setNullSettingAllowed(this.annotation.nullSettingAllowed());
            field.setTextChangeEventMode(this.annotation.textChangeEventMode());
            field.setTextChangeTimeout(this.annotation.textChangeTimeout());
            field.setColumns(this.annotation.columns());
            if (this.annotation.maxLength() != -1)
                field.setMaxLength(this.annotation.maxLength());
        }
    }

    /**
     * Applies properties from a {@link FieldBuilder.TextField} annotation to a {@link com.vaadin.ui.TextField}.
     */
    private static class TextFieldApplier extends AnnotationApplier<FieldBuilder.TextField, com.vaadin.ui.TextField> {

        public TextFieldApplier(FieldBuilder.TextField annotation) {
            super(annotation, com.vaadin.ui.TextField.class);
        }

        @Override
        public Class<? extends com.vaadin.ui.TextField> getActualFieldType() {
            return this.annotation.type();
        }

        @Override
        public void applyTo(com.vaadin.ui.TextField field) {
        }
    }

    /**
     * Applies properties from a {@link FieldBuilder.TextArea} annotation to a {@link com.vaadin.ui.TextArea}.
     */
    private static class TextAreaApplier extends AnnotationApplier<FieldBuilder.TextArea, com.vaadin.ui.TextArea> {

        public TextAreaApplier(FieldBuilder.TextArea annotation) {
            super(annotation, com.vaadin.ui.TextArea.class);
        }

        @Override
        public Class<? extends com.vaadin.ui.TextArea> getActualFieldType() {
            return this.annotation.type();
        }

        @Override
        public void applyTo(com.vaadin.ui.TextArea field) {
            field.setWordwrap(this.annotation.wordwrap());
            if (this.annotation.rows() != -1)
                field.setMaxLength(this.annotation.rows());

        }
    }

    /**
     * Applies properties from a {@link FieldBuilder.PasswordField} annotation to a {@link com.vaadin.ui.PasswordField}.
     */
    private static class PasswordFieldApplier extends AnnotationApplier<FieldBuilder.PasswordField, com.vaadin.ui.PasswordField> {

        public PasswordFieldApplier(FieldBuilder.PasswordField annotation) {
            super(annotation, com.vaadin.ui.PasswordField.class);
        }

        @Override
        public Class<? extends com.vaadin.ui.PasswordField> getActualFieldType() {
            return this.annotation.type();
        }

        @Override
        public void applyTo(com.vaadin.ui.PasswordField field) {
        }
    }

// Annotations

    /**
     * Specifies that the annotated method will return an {@link com.vaadin.ui.AbstractField} suitable for
     * editing the specified property.
     *
     * <p>
     * Annotated methods must take zero arguments and return a type compatible with {@link com.vaadin.ui.AbstractField}.
     * </p>
     *
     * @see FieldBuilder
     */
    @Retention(RetentionPolicy.RUNTIME)
    @Target(ElementType.METHOD)
    @Documented
    public @interface ProvidesField {

        /**
         * The name of the property that the annotated method's return value edits.
         */
        String value();
    }

    /**
     * Specifies how a Java property should be edited in Vaadin using an {@link com.vaadin.ui.AbstractField}.
     *
     * @see FieldBuilder
     */
    @Retention(RetentionPolicy.RUNTIME)
    @Target(ElementType.METHOD)
    @Documented
    public @interface AbstractField {

        /**
         * Get the {@link AbstractField} type that will edit the property.
         *
         * <p>
         * Although this property has a default value, it must be overridden either in this annotation, or
         * by also including a more specific annotation such as {@link TextField}.
         * </p>
         */
        @SuppressWarnings("rawtypes")
        Class<? extends com.vaadin.ui.AbstractField> type() default com.vaadin.ui.AbstractField.class;

        /**
         * Get style names.
         *
         * @see com.vaadin.ui.AbstractComponent#addStyleName
         */
        String[] styleNames() default {};

        /**
         * Get width.
         *
         * @see com.vaadin.ui.AbstractComponent#setWidth(String)
         */
        String width() default "";

        /**
         * Get height.
         *
         * @see com.vaadin.ui.AbstractComponent#setHeight(String)
         */
        String height() default "";

        /**
         * Get the caption associated with this field.
         *
         * @see com.vaadin.ui.AbstractComponent#setCaption
         */
        String caption() default "";

        /**
         * Get the description associated with this field.
         *
         * @see com.vaadin.ui.AbstractComponent#setDescription
         */
        String description() default "";

        /**
         * Get whether this field is enabled.
         *
         * @see com.vaadin.ui.AbstractComponent#setEnabled
         */
        boolean enabled() default true;

        /**
         * Get whether this field is immediate.
         *
         * @see com.vaadin.ui.AbstractComponent#setImmediate
         */
        boolean immediate() default false;

        /**
         * Get whether this field is read-only.
         *
         * @see com.vaadin.ui.AbstractComponent#setReadOnly
         */
        boolean readOnly() default false;

        /**
         * Get the {@link Converter} type that convert field value to data model type.
         * The specified class must have a no-arg constructor and compatible type.
         *
         * <p>
         * The default value of this property is {@link Converter}, which means do not set a specific
         * {@link Converter} on the field.
         * </p>
         *
         * @see AbstractField#setConverter(Converter)
         */
        @SuppressWarnings("rawtypes")
        Class<? extends Converter> converter() default Converter.class;

        /**
         * Get {@link Validator} types to add to this field. All such types must have no-arg constructors.
         *
         * @see AbstractField#addValidator
         */
        Class<? extends Validator>[] validators() default {};

        /**
         * Get whether this field is buffered.
         *
         * @see AbstractField#setBuffered
         */
        boolean buffered() default false;

        /**
         * Get whether invalid values are allowed.
         *
         * @see AbstractField#setInvalidAllowed
         */
        boolean invalidAllowed() default true;

        /**
         * Get whether invalid values should be committed.
         *
         * @see AbstractField#setInvalidCommitted
         */
        boolean invalidCommitted() default false;

        /**
         * Get error message when value cannot be converted to data model type.
         *
         * @see AbstractField#setConversionError
         */
        String conversionError() default "Could not convert value to {0}";

        /**
         * Get the error that is shown if this field is required, but empty.
         *
         * @see AbstractField#setRequiredError
         */
        String requiredError() default "";

        /**
         * Get whether automatic visible validation is enabled.
         *
         * @see AbstractField#setValidationVisible
         */
        boolean validationVisible() default true;

        /**
         * Get whether field is required.
         *
         * @see AbstractField#setRequired
         */
        boolean required() default false;

        /**
         * Get tabular index.
         *
         * @see AbstractField#setTabIndex
         */
        int tabIndex() default 0;
    }

    /**
     * Specifies how a Java property should be edited in Vaadin using an {@link com.vaadin.ui.AbstractSelect}.
     *
     * @see FieldBuilder.AbstractField
     * @see FieldBuilder
     */
    @Retention(RetentionPolicy.RUNTIME)
    @Target(ElementType.METHOD)
    @Documented
    public @interface AbstractSelect {

        /**
         * Get the {@link com.vaadin.ui.AbstractSelect} type that will edit the property.
         *
         * <p>
         * Although this property has a default value, it must be overridden either in this annotation, or
         * by also including a more specific annotation such as {@link ComboBox}.
         * </p>
         */
        Class<? extends com.vaadin.ui.AbstractSelect> type() default com.vaadin.ui.AbstractSelect.class;

        /**
         * Get the item caption mode.
         *
         * @see com.vaadin.ui.AbstractSelect#setItemCaptionMode
         */
        com.vaadin.ui.AbstractSelect.ItemCaptionMode itemCaptionMode()
          default com.vaadin.ui.AbstractSelect.ItemCaptionMode.EXPLICIT_DEFAULTS_ID;

        /**
         * Get the item caption property ID (which must be a string).
         *
         * @see com.vaadin.ui.AbstractSelect#setItemCaptionPropertyId
         */
        String itemCaptionPropertyId() default "";

        /**
         * Get the item icon property ID (which must be a string).
         *
         * @see com.vaadin.ui.AbstractSelect#setItemIconPropertyId
         */
        String itemIconPropertyId() default "";

        /**
         * Get the null selection item ID.
         *
         * @see com.vaadin.ui.AbstractSelect#setNullSelectionItemId
         */
        String nullSelectionItemId() default "";

        /**
         * Get multi-select setting.
         *
         * @see com.vaadin.ui.AbstractSelect#setMultiSelect
         */
        boolean multiSelect() default false;

        /**
         * Get whether new items are allowed.
         *
         * @see com.vaadin.ui.AbstractSelect#setNewItemsAllowed
         */
        boolean newItemsAllowed() default false;

        /**
         * Get whether null selection is allowed.
         *
         * @see com.vaadin.ui.AbstractSelect#setNullSelectionAllowed
         */
        boolean nullSelectionAllowed() default true;
    }

    /**
     * Specifies how a Java property should be edited in Vaadin using an {@link com.vaadin.ui.CheckBox}.
     *
     * @see FieldBuilder.AbstractField
     * @see FieldBuilder
     */
    @Retention(RetentionPolicy.RUNTIME)
    @Target(ElementType.METHOD)
    @Documented
    public @interface CheckBox {

        /**
         * Get the {@link com.vaadin.ui.CheckBox} type that will edit the property. Type must have a no-arg constructor.
         */
        Class<? extends com.vaadin.ui.CheckBox> type() default com.vaadin.ui.CheckBox.class;
    }

    /**
     * Specifies how a Java property should be edited in Vaadin using an {@link com.vaadin.ui.ComboBox}.
     *
     * @see FieldBuilder.AbstractField
     * @see FieldBuilder.AbstractSelect
     * @see FieldBuilder
     */
    @Retention(RetentionPolicy.RUNTIME)
    @Target(ElementType.METHOD)
    @Documented
    public @interface ComboBox {

        /**
         * Get the {@link com.vaadin.ui.ComboBox} type that will edit the property. Type must have a no-arg constructor.
         */
        Class<? extends com.vaadin.ui.ComboBox> type() default com.vaadin.ui.ComboBox.class;

        /**
         * Get the input prompt.
         *
         * @see com.vaadin.ui.ComboBox#setInputPrompt
         */
        String inputPrompt() default "";

        /**
         * Get the page length.
         *
         * @see com.vaadin.ui.ComboBox#setPageLength
         */
        int pageLength() default -1;

        /**
         * Get whether to scroll to the selected item.
         *
         * @see com.vaadin.ui.ComboBox#setScrollToSelectedItem
         */
        boolean scrollToSelectedItem() default true;

        /**
         * Get whether text input is allowed.
         *
         * @see com.vaadin.ui.ComboBox#setTextInputAllowed
         */
        boolean textInputAllowed() default true;

        /**
         * Get the filtering mode.
         *
         * @see com.vaadin.ui.ComboBox#setFilteringMode
         */
        FilteringMode filteringMode() default FilteringMode.STARTSWITH;
    }

    /**
     * Specifies how a Java property should be edited in Vaadin using an {@link com.vaadin.ui.ListSelect}.
     *
     * @see FieldBuilder.AbstractField
     * @see FieldBuilder.AbstractSelect
     * @see FieldBuilder
     */
    @Retention(RetentionPolicy.RUNTIME)
    @Target(ElementType.METHOD)
    @Documented
    public @interface ListSelect {

        /**
         * Get the {@link com.vaadin.ui.ListSelect} type that will edit the property. Type must have a no-arg constructor.
         */
        Class<? extends com.vaadin.ui.ListSelect> type() default com.vaadin.ui.ListSelect.class;

        /**
         * Get the number of rows in the editor.
         *
         * @see com.vaadin.ui.ListSelect#setRows
         */
        int rows() default -1;
    }

    /**
     * Specifies how a Java property should be edited in Vaadin using an {@link com.vaadin.ui.DateField}.
     *
     * @see FieldBuilder.AbstractField
     * @see FieldBuilder
     */
    @Retention(RetentionPolicy.RUNTIME)
    @Target(ElementType.METHOD)
    @Documented
    public @interface DateField {

        /**
         * Get the {@link com.vaadin.ui.DateField} type that will edit the property. Type must have a no-arg constructor.
         */
        Class<? extends com.vaadin.ui.DateField> type() default com.vaadin.ui.DateField.class;

        /**
         * Get the date format string.
         *
         * @see com.vaadin.ui.DateField#setDateFormat
         */
        String dateFormat() default "";

        /**
         * Get the date parse error message.
         *
         * @see com.vaadin.ui.DateField#setParseErrorMessage
         */
        String parseErrorMessage() default "";

        /**
         * Get the date out of range error message.
         *
         * @see com.vaadin.ui.DateField#setDateOutOfRangeMessage
         */
        String dateOutOfRangeMessage() default "";

        /**
         * Get the date resolution.
         *
         * @see com.vaadin.ui.DateField#setResolution
         */
        Resolution resolution() default Resolution.DAY;

        /**
         * Get whether to show ISO week numbers.
         *
         * @see com.vaadin.ui.DateField#setShowISOWeekNumbers
         */
        boolean showISOWeekNumbers() default false;

        /**
         * Get the time zone (in string form).
         *
         * @see com.vaadin.ui.DateField#setTimeZone
         */
        String timeZone() default "";

        /**
         * Get lenient mode.
         *
         * @see com.vaadin.ui.DateField#setLenient
         */
        boolean lenient() default false;
    }

    /**
     * Specifies how a Java property should be edited in Vaadin using a {@link com.vaadin.ui.AbstractTextField}.
     *
     * @see FieldBuilder.AbstractField
     * @see FieldBuilder
     */
    @Retention(RetentionPolicy.RUNTIME)
    @Target(ElementType.METHOD)
    @Documented
    public @interface AbstractTextField {

        /**
         * Get the {@link AbstractTextField} type that will edit the property.
         */
        Class<? extends com.vaadin.ui.TextField> type() default com.vaadin.ui.TextField.class;

        /**
         * Get the representation of null.
         *
         * @see com.vaadin.ui.AbstractTextField#setNullRepresentation
         */
        String nullRepresentation() default "null";

        /**
         * Get whether null value may be set.
         *
         * @see com.vaadin.ui.AbstractTextField#setNullSettingAllowed
         */
        boolean nullSettingAllowed() default false;

        /**
         * Get text change event mode.
         *
         * @see com.vaadin.ui.AbstractTextField#setTextChangeEventMode
         */
        com.vaadin.ui.AbstractTextField.TextChangeEventMode textChangeEventMode()
          default com.vaadin.ui.AbstractTextField.TextChangeEventMode.LAZY;

        /**
         * Get text change event timeout.
         *
         * @see com.vaadin.ui.AbstractTextField#setTextChangeTimeout
         */
        int textChangeTimeout() default -1;

        /**
         * Get the number of columns.
         *
         * @see com.vaadin.ui.AbstractTextField#setColumns
         */
        int columns() default 0;

        /**
         * Get the maximum length.
         *
         * @see com.vaadin.ui.AbstractTextField#setMaxLength
         */
        int maxLength() default -1;
    }

    /**
     * Specifies how a Java property should be edited in Vaadin using a {@link com.vaadin.ui.TextField}.
     *
     * @see FieldBuilder.AbstractField
     * @see FieldBuilder.AbstractTextField
     * @see FieldBuilder
     */
    @Retention(RetentionPolicy.RUNTIME)
    @Target(ElementType.METHOD)
    @Documented
    public @interface TextField {

        /**
         * Get the {@link TextField} type that will edit the property.
         */
        Class<? extends com.vaadin.ui.TextField> type() default com.vaadin.ui.TextField.class;
    }

    /**
     * Specifies how a Java property should be edited in Vaadin using a {@link com.vaadin.ui.TextArea}.
     *
     * @see FieldBuilder.AbstractField
     * @see FieldBuilder.AbstractTextField
     * @see FieldBuilder
     */
    @Retention(RetentionPolicy.RUNTIME)
    @Target(ElementType.METHOD)
    @Documented
    public @interface TextArea {

        /**
         * Get the {@link TextArea} type that will edit the property.
         */
        Class<? extends com.vaadin.ui.TextArea> type() default com.vaadin.ui.TextArea.class;

        /**
         * Set the number of rows.
         *
         * @see com.vaadin.ui.TextArea#setRows
         */
        int rows() default -1;

        /**
         * Set wordwrap mode.
         *
         * @see com.vaadin.ui.TextArea#setWordwrap
         */
        boolean wordwrap() default true;
    }

    /**
     * Specifies how a Java property should be edited in Vaadin using a {@link com.vaadin.ui.PasswordField}.
     *
     * @see FieldBuilder.AbstractField
     * @see FieldBuilder.AbstractTextField
     * @see FieldBuilder
     */
    @Retention(RetentionPolicy.RUNTIME)
    @Target(ElementType.METHOD)
    @Documented
    public @interface PasswordField {

        /**
         * Get the {@link PasswordField} type that will edit the property.
         */
        Class<? extends com.vaadin.ui.PasswordField> type() default com.vaadin.ui.PasswordField.class;
    }
}

