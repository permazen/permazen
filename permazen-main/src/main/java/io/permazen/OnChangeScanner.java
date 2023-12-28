
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen;

import com.google.common.base.Converter;
import com.google.common.reflect.TypeToken;

import io.permazen.annotation.OnChange;
import io.permazen.change.FieldChange;
import io.permazen.change.ListFieldAdd;
import io.permazen.change.ListFieldClear;
import io.permazen.change.ListFieldRemove;
import io.permazen.change.ListFieldReplace;
import io.permazen.change.MapFieldAdd;
import io.permazen.change.MapFieldClear;
import io.permazen.change.MapFieldRemove;
import io.permazen.change.MapFieldReplace;
import io.permazen.change.SetFieldAdd;
import io.permazen.change.SetFieldClear;
import io.permazen.change.SetFieldRemove;
import io.permazen.change.SimpleFieldChange;
import io.permazen.core.Field;
import io.permazen.core.ListField;
import io.permazen.core.MapField;
import io.permazen.core.ObjId;
import io.permazen.core.SetField;
import io.permazen.core.SimpleField;
import io.permazen.core.Transaction;
import io.permazen.core.TypeNotInSchemaException;
import io.permazen.core.UnknownFieldException;

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.NavigableSet;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Scans for {@link OnChange &#64;OnChange} annotations.
 */
class OnChangeScanner<T> extends AnnotationScanner<T, OnChange> {

    OnChangeScanner(JClass<T> jclass) {
        super(jclass, OnChange.class);
    }

    @Override
    protected boolean includeMethod(Method method, OnChange annotation) {
        this.checkReturnType(method, void.class);
        if (this.getParameterTypeTokens(method).size() != 1) {
            throw new IllegalArgumentException(String.format(
              "%s: method is required to take exactly one parameter", this.getErrorPrefix(method)));
        }
        return true;                                    // we do further parameter type check in ChangeMethodInfo
    }

    @Override
    protected ChangeMethodInfo createMethodInfo(Method method, OnChange annotation) {
        return new ChangeMethodInfo(method, annotation);
    }

// ChangeMethodInfo

    class ChangeMethodInfo extends MethodInfo implements AllChangesListener {

        final HashSet<Integer> targetFieldStorageIds = new HashSet<>();
        final ReferencePath path;
        final Class<?>[] genericTypes;

        ChangeMethodInfo(Method method, OnChange annotation) {
            super(method, annotation);

            // Get database
            final Permazen jdb = OnChangeScanner.this.jclass.jdb;
            final String errorPrefix = OnChangeScanner.this.getErrorPrefix(method);

            // Parse reference path
            try {
                this.path = jdb.parseReferencePath(method.getDeclaringClass(), annotation.path());
            } catch (IllegalArgumentException e) {
                throw new IllegalArgumentException(String.format("%s: %s", errorPrefix, e.getMessage()), e);
            }

            // Get target object types
            final Set<JClass<?>> targetTypes = path.getTargetTypes();

            // Get method parameter type (generic and raw)
            final Class<?> rawParameterType = method.getParameterTypes()[0];
            final TypeToken<?> genericParameterType = OnChangeScanner.this.getParameterTypeTokens(method).get(0);

            // Extract generic types from the FieldChange<?> parameter
            final Type firstParameterType = method.getGenericParameterTypes()[0];
            if (firstParameterType instanceof ParameterizedType) {
                final ArrayList<Class<?>> genericTypeList = new ArrayList<>(3);
                for (Type type : ((ParameterizedType)firstParameterType).getActualTypeArguments())
                    genericTypeList.add(TypeToken.of(type).getRawType());
                this.genericTypes = genericTypeList.toArray(new Class<?>[genericTypeList.size()]);
            } else
                this.genericTypes = new Class<?>[] { rawParameterType };

            // Wildcard field names?
            final boolean wildcard = annotation.value().length == 0;

            // Track which fields (a) were found, and (b) emit change events compatible with method parameter type
            final Set<String> fieldsNotFound = new LinkedHashSet<>(Arrays.asList(annotation.value()));
            final Set<String> fieldsNotMatched = new LinkedHashSet<>(Arrays.asList(annotation.value()));

            // Iterate over all target object types
            for (JClass<?> jclass : targetTypes) {

                // Get field list, but replace an empty list with every notifying field in the target object type
                final List<String> fieldNames = wildcard ?
                  jclass.jfieldsByName.values().stream()
                    .filter(JField::supportsChangeNotifications)
                    .map(JField::getName)
                    .collect(Collectors.toList()) :
                  Arrays.asList(annotation.value());

                // Iterate over target fields
                for (String fieldName : fieldNames) {

                    // Find the field in this cursor's target object type
                    final JField jfield;
                    try {
                        jfield = Util.findField(jclass, fieldName);
                    } catch (IllegalArgumentException e) {
                        throw new IllegalArgumentException(String.format("%s: %s", errorPrefix, e.getMessage()), e);
                    }

                    // Not found?
                    if (jfield == null)
                        continue;
                    fieldsNotFound.remove(fieldName);

                    // Gather its possible change event types
                    final ArrayList<TypeToken<?>> possibleChangeTypes = new ArrayList<TypeToken<?>>();
                    try {
                        jfield.addChangeParameterTypes(possibleChangeTypes, jclass.getType());
                    } catch (UnsupportedOperationException e) {
                        throw new IllegalArgumentException(String.format(
                          "%s: %s in %s does not support change notifications", errorPrefix, jfield, jclass));
                    }

                    // Check whether method parameter type matches as least one of them; it must do so consistently raw vs. generic
                    boolean matched = false;
                    for (TypeToken<?> possibleChangeType : possibleChangeTypes) {
                        final boolean matchesGeneric = genericParameterType.isSupertypeOf(possibleChangeType);
                        final boolean matchesRaw = rawParameterType.isAssignableFrom(possibleChangeType.getRawType());
                        assert !matchesGeneric || matchesRaw;
                        if (matchesGeneric != matchesRaw) {
                            throw new IllegalArgumentException(String.format(
                              "%s: parameter type %s will match change events of type %s from field \"%s\" at runtime"
                              + " due to type erasure, but its generic type is does not match %s; try narrowing or"
                              + " widening the parameter type while keeping it compatible with %s",
                              errorPrefix, genericParameterType, possibleChangeType, fieldName, possibleChangeType,
                              possibleChangeTypes.size() != 1 ?
                                "one or more of: " + possibleChangeTypes : possibleChangeTypes.get(0)));
                        }
                        matched |= matchesGeneric;
                    }

                    // Not matched?
                    if (!matched)
                        continue;
                    fieldsNotMatched.remove(fieldName);

                    // Configure monitoring for this field
                    this.targetFieldStorageIds.add(jfield.storageId);
                }
            }

            // Check for bogus field names (non-wildcard only)
            final Iterator<String> fieldsNotFoundIterator = fieldsNotFound.iterator();
            if (fieldsNotFoundIterator.hasNext()) {
                final String fieldName = fieldsNotFoundIterator.next();
                throw new IllegalArgumentException(String.format(
                  "%s: field \"%s\" not found in %s", errorPrefix, fieldName,
                  this.path.isEmpty() ? method.getDeclaringClass() :
                  targetTypes.size() == 1 ? targetTypes.iterator().next() :
                  "any of " + targetTypes));
            }

            // Check for valid field names that didn't match event type (non-wildcard only)
            final Iterator<String> fieldsNotMatchedIterator = fieldsNotMatched.iterator();
            if (fieldsNotMatchedIterator.hasNext()) {
                final String fieldName = fieldsNotMatchedIterator.next();
                throw new IllegalArgumentException(String.format(
                  "%s: field \"%s\" doesn't generate any change events matching the method's parameter type %s",
                  errorPrefix, fieldName, genericParameterType));
            }

            // Check for wildcard with no matches
            if (this.targetFieldStorageIds.isEmpty()) {
                throw new IllegalArgumentException(String.format(
                  "%s: there are no fields that will generate change events matching the method's parameter type %s",
                  errorPrefix, genericParameterType));
            }
        }

        // Register listeners for this method
        void registerChangeListener(Transaction tx) {
            for (int storageId : this.targetFieldStorageIds)
                tx.addFieldChangeListener(storageId, path.getReferenceFields(), path.getPathKeyRanges(), this);
        }

        // Note genericTypes is derived from this.method, so there's no need to include it in equals() or hashCode()
        @Override
        public boolean equals(Object obj) {
            if (obj == this)
                return true;
            if (!super.equals(obj))
                return false;
            final OnChangeScanner<?>.ChangeMethodInfo that = (OnChangeScanner<?>.ChangeMethodInfo)obj;
            return this.path.equals(that.path) && this.targetFieldStorageIds.equals(that.targetFieldStorageIds);
        }

        @Override
        public int hashCode() {
            return super.hashCode()
              ^ this.targetFieldStorageIds.hashCode()
              ^ this.path.hashCode();
        }

    // SimpleFieldChangeListener

        @Override
        @SuppressWarnings({ "unchecked", "rawtypes" })
        public <T> void onSimpleFieldChange(Transaction tx, ObjId id,
          SimpleField<T> field, int[] path, NavigableSet<ObjId> referrers, T oldValue, T newValue) {
            final JTransaction jtx = (JTransaction)tx.getUserObject();
            assert jtx != null && jtx.tx == tx;
            final JSimpleField jfield = this.getJField(jtx, id, field, JSimpleField.class);
            if (jfield == null)
                return;
            final Object joldValue = this.convertCoreValue(jtx, jfield, oldValue);
            final Object jnewValue = this.convertCoreValue(jtx, jfield, newValue);
            final JObject jobj = this.checkTypes(jtx, SimpleFieldChange.class, id, joldValue, jnewValue);
            if (jobj == null)
                return;
            this.invoke(jtx, referrers, new SimpleFieldChange(jobj, jfield.name, joldValue, jnewValue));
        }

    // SetFieldChangeListener

        @Override
        @SuppressWarnings({ "unchecked", "rawtypes" })
        public <E> void onSetFieldAdd(Transaction tx, ObjId id,
          SetField<E> field, int[] path, NavigableSet<ObjId> referrers, E value) {
            final JTransaction jtx = (JTransaction)tx.getUserObject();
            assert jtx != null && jtx.tx == tx;
            final JSetField jfield = this.getJField(jtx, id, field, JSetField.class);
            if (jfield == null)
                return;
            final Object jvalue = this.convertCoreValue(jtx, jfield.elementField, value);
            final JObject jobj = this.checkTypes(jtx, SetFieldAdd.class, id, jvalue);
            if (jobj == null)
                return;
            this.invoke(jtx, referrers, new SetFieldAdd(jobj, jfield.name, jvalue));
        }

        @Override
        @SuppressWarnings({ "unchecked", "rawtypes" })
        public <E> void onSetFieldRemove(Transaction tx, ObjId id,
          SetField<E> field, int[] path, NavigableSet<ObjId> referrers, E value) {
            final JTransaction jtx = (JTransaction)tx.getUserObject();
            assert jtx != null && jtx.tx == tx;
            final JSetField jfield = this.getJField(jtx, id, field, JSetField.class);
            if (jfield == null)
                return;
            final Object jvalue = this.convertCoreValue(jtx, jfield.elementField, value);
            final JObject jobj = this.checkTypes(jtx, SetFieldRemove.class, id, jvalue);
            if (jobj == null)
                return;
            this.invoke(jtx, referrers, new SetFieldRemove(jobj, jfield.name, jvalue));
        }

        @Override
        public void onSetFieldClear(Transaction tx, ObjId id, SetField<?> field, int[] path, NavigableSet<ObjId> referrers) {
            final JTransaction jtx = (JTransaction)tx.getUserObject();
            assert jtx != null && jtx.tx == tx;
            final JSetField jfield = this.getJField(jtx, id, field, JSetField.class);
            if (jfield == null)
                return;
            final JObject jobj = this.checkTypes(jtx, SetFieldClear.class, id);
            if (jobj == null)
                return;
            this.invoke(jtx, referrers, new SetFieldClear<>(jobj, jfield.name));
        }

    // ListFieldChangeListener

        @Override
        @SuppressWarnings({ "unchecked", "rawtypes" })
        public <E> void onListFieldAdd(Transaction tx, ObjId id,
          ListField<E> field, int[] path, NavigableSet<ObjId> referrers, int index, E value) {
            final JTransaction jtx = (JTransaction)tx.getUserObject();
            assert jtx != null && jtx.tx == tx;
            final JListField jfield = this.getJField(jtx, id, field, JListField.class);
            if (jfield == null)
                return;
            final Object jvalue = this.convertCoreValue(jtx, jfield.elementField, value);
            final JObject jobj = this.checkTypes(jtx, ListFieldAdd.class, id, jvalue);
            if (jobj == null)
                return;
            this.invoke(jtx, referrers, new ListFieldAdd(jobj, jfield.name, index, jvalue));
        }

        @Override
        @SuppressWarnings({ "unchecked", "rawtypes" })
        public <E> void onListFieldRemove(Transaction tx, ObjId id,
          ListField<E> field, int[] path, NavigableSet<ObjId> referrers, int index, E value) {
            final JTransaction jtx = (JTransaction)tx.getUserObject();
            assert jtx != null && jtx.tx == tx;
            final JListField jfield = this.getJField(jtx, id, field, JListField.class);
            if (jfield == null)
                return;
            final Object jvalue = this.convertCoreValue(jtx, jfield.elementField, value);
            final JObject jobj = this.checkTypes(jtx, ListFieldRemove.class, id, jvalue);
            if (jobj == null)
                return;
            this.invoke(jtx, referrers, new ListFieldRemove(jobj, jfield.name, index, jvalue));
        }

        @Override
        @SuppressWarnings({ "unchecked", "rawtypes" })
        public <E> void onListFieldReplace(Transaction tx, ObjId id,
          ListField<E> field, int[] path, NavigableSet<ObjId> referrers, int index, E oldValue, E newValue) {
            final JTransaction jtx = (JTransaction)tx.getUserObject();
            assert jtx != null && jtx.tx == tx;
            final JListField jfield = this.getJField(jtx, id, field, JListField.class);
            if (jfield == null)
                return;
            final Object joldValue = this.convertCoreValue(jtx, jfield.elementField, oldValue);
            final Object jnewValue = this.convertCoreValue(jtx, jfield.elementField, newValue);
            final JObject jobj = this.checkTypes(jtx, ListFieldReplace.class, id, joldValue, jnewValue);
            if (jobj == null)
                return;
            this.invoke(jtx, referrers, new ListFieldReplace(jobj, jfield.name, index, joldValue, jnewValue));
        }

        @Override
        public void onListFieldClear(Transaction tx, ObjId id, ListField<?> field, int[] path, NavigableSet<ObjId> referrers) {
            final JTransaction jtx = (JTransaction)tx.getUserObject();
            assert jtx != null && jtx.tx == tx;
            final JListField jfield = this.getJField(jtx, id, field, JListField.class);
            if (jfield == null)
                return;
            final JObject jobj = this.checkTypes(jtx, ListFieldClear.class, id);
            if (jobj == null)
                return;
            this.invoke(jtx, referrers, new ListFieldClear<>(jobj, jfield.name));
        }

    // MapFieldChangeListener

        @Override
        @SuppressWarnings({ "unchecked", "rawtypes" })
        public <K, V> void onMapFieldAdd(Transaction tx, ObjId id,
          MapField<K, V> field, int[] path, NavigableSet<ObjId> referrers, K key, V value) {
            final JTransaction jtx = (JTransaction)tx.getUserObject();
            assert jtx != null && jtx.tx == tx;
            final JMapField jfield = this.getJField(jtx, id, field, JMapField.class);
            if (jfield == null)
                return;
            final Object jkey = this.convertCoreValue(jtx, jfield.keyField, key);
            final Object jvalue = this.convertCoreValue(jtx, jfield.valueField, value);
            final JObject jobj = this.checkTypes(jtx, MapFieldAdd.class, id, jkey, jvalue);
            if (jobj == null)
                return;
            this.invoke(jtx, referrers, new MapFieldAdd(jobj, jfield.name, jkey, jvalue));
        }

        @Override
        @SuppressWarnings({ "unchecked", "rawtypes" })
        public <K, V> void onMapFieldRemove(Transaction tx, ObjId id,
          MapField<K, V> field, int[] path, NavigableSet<ObjId> referrers, K key, V value) {
            final JTransaction jtx = (JTransaction)tx.getUserObject();
            assert jtx != null && jtx.tx == tx;
            final JMapField jfield = this.getJField(jtx, id, field, JMapField.class);
            if (jfield == null)
                return;
            final Object jkey = this.convertCoreValue(jtx, jfield.keyField, key);
            final Object jvalue = this.convertCoreValue(jtx, jfield.valueField, value);
            final JObject jobj = this.checkTypes(jtx, MapFieldRemove.class, id, jkey, jvalue);
            if (jobj == null)
                return;
            this.invoke(jtx, referrers, new MapFieldRemove(jobj, jfield.name, jkey, jvalue));
        }

        @Override
        @SuppressWarnings({ "unchecked", "rawtypes" })
        public <K, V> void onMapFieldReplace(Transaction tx, ObjId id,
          MapField<K, V> field, int[] path, NavigableSet<ObjId> referrers, K key, V oldValue, V newValue) {
            final JTransaction jtx = (JTransaction)tx.getUserObject();
            assert jtx != null && jtx.tx == tx;
            final JMapField jfield = this.getJField(jtx, id, field, JMapField.class);
            if (jfield == null)
                return;
            final Object jkey = this.convertCoreValue(jtx, jfield.keyField, key);
            final Object joldValue = this.convertCoreValue(jtx, jfield.valueField, oldValue);
            final Object jnewValue = this.convertCoreValue(jtx, jfield.valueField, newValue);
            final JObject jobj = this.checkTypes(jtx, MapFieldReplace.class, id, jkey, joldValue, jnewValue);
            if (jobj == null)
                return;
            this.invoke(jtx, referrers,
              new MapFieldReplace(jobj, jfield.name, jkey, joldValue, jnewValue));
        }

        @Override
        public void onMapFieldClear(Transaction tx, ObjId id, MapField<?, ?> field, int[] path, NavigableSet<ObjId> referrers) {
            final JTransaction jtx = (JTransaction)tx.getUserObject();
            assert jtx != null && jtx.tx == tx;
            final JMapField jfield = this.getJField(jtx, id, field, JMapField.class);
            if (jfield == null)
                return;
            final JObject jobj = this.checkTypes(jtx, MapFieldClear.class, id);
            if (jobj == null)
                return;
            this.invoke(jtx, referrers, new MapFieldClear<>(jobj, jfield.name));
        }

    // Internal methods

        private <T extends JField> T getJField(JTransaction jtx, ObjId id, Field<?> field, Class<T> type) {
            try {
                return jtx.jdb.getJField(id, field.getName(), type);
            } catch (TypeNotInSchemaException | UnknownFieldException e) {
                return null;        // somebody changed the field directly via the core API without first upgrading the object
            }
        }

        @SuppressWarnings({ "rawtypes", "unchecked" })
        private Object convertCoreValue(JTransaction jtx, JSimpleField jfield, Object value) {
            final Converter converter = jfield.getConverter(jtx);
            return converter != null ? converter.convert(value) : value;
        }

        private JObject checkTypes(JTransaction jtx, Class<? /*extends FieldChange<?>*/> changeType, ObjId id, Object... values) {

            // Check method parameter type
            final Method method = this.getMethod();
            if (!method.getParameterTypes()[0].isAssignableFrom(changeType))
                return null;

            // Check first generic type parameter which is the JObject corresponding to id
            final JObject jobj = jtx.get(id);
            if (!this.genericTypes[0].isInstance(jobj))
                return null;

            // Check other generic type parameter(s)
            for (int i = 1; i < this.genericTypes.length; i++) {
                final Object value = values[Math.min(i, values.length) - 1];
                if (value != null && !this.genericTypes[i].isInstance(value))
                    return null;
            }

            // OK types agree
            return jobj;
        }

        // Invoke the @OnChange method
        private void invoke(JTransaction jtx, NavigableSet<ObjId> referrers, FieldChange<JObject> change) {
            assert change != null;
            final Method method = this.getMethod();
            if ((method.getModifiers() & Modifier.STATIC) != 0)
                Util.invoke(method, null, change);
            else {
                for (ObjId id : referrers) {
                    final JObject target = jtx.get(id);             // type of 'id' should always be found

                    // Avoid invoking subclass's @OnChange method on superclass instance;
                    // this can happen when the field is in superclass but wildcard @OnChange is in the subclass
                    if (method.getDeclaringClass().isInstance(target))
                        Util.invoke(method, target, change);
                }
            }
        }
    }
}
