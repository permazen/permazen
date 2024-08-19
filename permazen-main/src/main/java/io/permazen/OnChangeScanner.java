
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

    OnChangeScanner(PermazenClass<T> pclass) {
        super(pclass, OnChange.class);
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
            final Permazen pdb = OnChangeScanner.this.pclass.pdb;
            final String errorPrefix = OnChangeScanner.this.getErrorPrefix(method);

            // Path must be empty if method is static
            if ((method.getModifiers() & Modifier.STATIC) != 0 && !annotation.path().isEmpty()) {
                throw new IllegalArgumentException(String.format(
                  "%s: method is static so @%s.path() must be empty",
                  errorPrefix, annotation.annotationType().getSimpleName()));
            }

            // Parse reference path
            try {
                this.path = pdb.parseReferencePath(method.getDeclaringClass(), annotation.path());
            } catch (IllegalArgumentException e) {
                throw new IllegalArgumentException(String.format("%s: %s", errorPrefix, e.getMessage()), e);
            }

            // Get target object types
            final Set<PermazenClass<?>> targetTypes = path.getTargetTypes();

            // Get method parameter type (generic and raw)
            final TypeToken<?> genericParameterType = OnChangeScanner.this.getParameterTypeTokens(method).get(0);

            // Extract generic types from the FieldChange<?> parameter
            final Type firstParameterType = method.getGenericParameterTypes()[0];
            if (firstParameterType instanceof ParameterizedType) {
                final ArrayList<Class<?>> genericTypeList = new ArrayList<>(3);
                for (Type type : ((ParameterizedType)firstParameterType).getActualTypeArguments())
                    genericTypeList.add(TypeToken.of(type).getRawType());
                this.genericTypes = genericTypeList.toArray(new Class<?>[genericTypeList.size()]);
            } else
                this.genericTypes = new Class<?>[] { genericParameterType.getRawType() };

            // Wildcard field names?
            final boolean wildcard = annotation.value().length == 0;

            // Track which fields (a) were found, and (b) emit change events compatible with method parameter type
            final Set<String> fieldsNotFound = new LinkedHashSet<>(Arrays.asList(annotation.value()));
            final Set<String> fieldsNotMatched = new LinkedHashSet<>(Arrays.asList(annotation.value()));

            // Iterate over all target object types
            for (PermazenClass<?> pclass : targetTypes) {

                // Get field list, but replace an empty list with every notifying field in the target object type
                final List<String> fieldNames = wildcard ?
                  pclass.fieldsByName.values().stream()
                    .filter(PermazenField::supportsChangeNotifications)
                    .map(PermazenField::getName)
                    .collect(Collectors.toList()) :
                  Arrays.asList(annotation.value());

                // Iterate over target fields
                for (String fieldName : fieldNames) {

                    // Find the field in this cursor's target object type
                    final PermazenField pfield;
                    try {
                        pfield = Util.findField(pclass, fieldName);
                    } catch (IllegalArgumentException e) {
                        throw new IllegalArgumentException(String.format("%s: %s", errorPrefix, e.getMessage()), e);
                    }

                    // Not found?
                    if (pfield == null)
                        continue;
                    fieldsNotFound.remove(fieldName);

                    // Gather its possible change event types
                    final ArrayList<TypeToken<?>> possibleChangeTypes = new ArrayList<TypeToken<?>>();
                    try {
                        pfield.addChangeParameterTypes(possibleChangeTypes, pclass.getType());
                    } catch (UnsupportedOperationException e) {
                        throw new IllegalArgumentException(String.format(
                          "%s: %s in %s does not support change notifications", errorPrefix, pfield, pclass));
                    }

                    // Verify the method parameter type matches event types consistently whether raw vs. generic
                    final TypeToken<?> mismatchType = Util.findErasureDifference(genericParameterType, possibleChangeTypes);
                    if (mismatchType != null) {
                        throw new IllegalArgumentException(String.format(
                          "%s: parameter type %s will match change events of type %s from field \"%s\" at runtime"
                          + " due to type erasure, but its generic type is does not match %s; try narrowing or"
                          + " widening the parameter type while keeping it compatible with %s",
                          errorPrefix, genericParameterType, mismatchType, fieldName, mismatchType,
                          possibleChangeTypes.size() != 1 ?
                            "one or more of: " + possibleChangeTypes : possibleChangeTypes.get(0)));
                    }

                    // If no event types match, this field name does not match
                    if (possibleChangeTypes.stream()
                      .map(TypeToken::getRawType)
                      .noneMatch(genericParameterType.getRawType()::isAssignableFrom))
                        continue;
                    fieldsNotMatched.remove(fieldName);

                    // Configure monitoring for this field
                    this.targetFieldStorageIds.add(pfield.storageId);
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
            final PermazenTransaction ptx = (PermazenTransaction)tx.getUserObject();
            assert ptx != null && ptx.tx == tx;
            final PermazenSimpleField pfield = this.getField(ptx, id, field, PermazenSimpleField.class);
            if (pfield == null)
                return;
            final Object poldValue = this.convertCoreValue(ptx, pfield, oldValue);
            final Object pnewValue = this.convertCoreValue(ptx, pfield, newValue);
            final PermazenObject pobj = this.checkTypes(ptx, SimpleFieldChange.class, id, poldValue, pnewValue);
            if (pobj == null)
                return;
            this.invoke(ptx, referrers, new SimpleFieldChange(pobj, pfield.name, poldValue, pnewValue));
        }

    // SetFieldChangeListener

        @Override
        @SuppressWarnings({ "unchecked", "rawtypes" })
        public <E> void onSetFieldAdd(Transaction tx, ObjId id,
          SetField<E> field, int[] path, NavigableSet<ObjId> referrers, E value) {
            final PermazenTransaction ptx = (PermazenTransaction)tx.getUserObject();
            assert ptx != null && ptx.tx == tx;
            final PermazenSetField pfield = this.getField(ptx, id, field, PermazenSetField.class);
            if (pfield == null)
                return;
            final Object pvalue = this.convertCoreValue(ptx, pfield.elementField, value);
            final PermazenObject pobj = this.checkTypes(ptx, SetFieldAdd.class, id, pvalue);
            if (pobj == null)
                return;
            this.invoke(ptx, referrers, new SetFieldAdd(pobj, pfield.name, pvalue));
        }

        @Override
        @SuppressWarnings({ "unchecked", "rawtypes" })
        public <E> void onSetFieldRemove(Transaction tx, ObjId id,
          SetField<E> field, int[] path, NavigableSet<ObjId> referrers, E value) {
            final PermazenTransaction ptx = (PermazenTransaction)tx.getUserObject();
            assert ptx != null && ptx.tx == tx;
            final PermazenSetField pfield = this.getField(ptx, id, field, PermazenSetField.class);
            if (pfield == null)
                return;
            final Object pvalue = this.convertCoreValue(ptx, pfield.elementField, value);
            final PermazenObject pobj = this.checkTypes(ptx, SetFieldRemove.class, id, pvalue);
            if (pobj == null)
                return;
            this.invoke(ptx, referrers, new SetFieldRemove(pobj, pfield.name, pvalue));
        }

        @Override
        public void onSetFieldClear(Transaction tx, ObjId id, SetField<?> field, int[] path, NavigableSet<ObjId> referrers) {
            final PermazenTransaction ptx = (PermazenTransaction)tx.getUserObject();
            assert ptx != null && ptx.tx == tx;
            final PermazenSetField pfield = this.getField(ptx, id, field, PermazenSetField.class);
            if (pfield == null)
                return;
            final PermazenObject pobj = this.checkTypes(ptx, SetFieldClear.class, id);
            if (pobj == null)
                return;
            this.invoke(ptx, referrers, new SetFieldClear<>(pobj, pfield.name));
        }

    // ListFieldChangeListener

        @Override
        @SuppressWarnings({ "unchecked", "rawtypes" })
        public <E> void onListFieldAdd(Transaction tx, ObjId id,
          ListField<E> field, int[] path, NavigableSet<ObjId> referrers, int index, E value) {
            final PermazenTransaction ptx = (PermazenTransaction)tx.getUserObject();
            assert ptx != null && ptx.tx == tx;
            final PermazenListField pfield = this.getField(ptx, id, field, PermazenListField.class);
            if (pfield == null)
                return;
            final Object pvalue = this.convertCoreValue(ptx, pfield.elementField, value);
            final PermazenObject pobj = this.checkTypes(ptx, ListFieldAdd.class, id, pvalue);
            if (pobj == null)
                return;
            this.invoke(ptx, referrers, new ListFieldAdd(pobj, pfield.name, index, pvalue));
        }

        @Override
        @SuppressWarnings({ "unchecked", "rawtypes" })
        public <E> void onListFieldRemove(Transaction tx, ObjId id,
          ListField<E> field, int[] path, NavigableSet<ObjId> referrers, int index, E value) {
            final PermazenTransaction ptx = (PermazenTransaction)tx.getUserObject();
            assert ptx != null && ptx.tx == tx;
            final PermazenListField pfield = this.getField(ptx, id, field, PermazenListField.class);
            if (pfield == null)
                return;
            final Object pvalue = this.convertCoreValue(ptx, pfield.elementField, value);
            final PermazenObject pobj = this.checkTypes(ptx, ListFieldRemove.class, id, pvalue);
            if (pobj == null)
                return;
            this.invoke(ptx, referrers, new ListFieldRemove(pobj, pfield.name, index, pvalue));
        }

        @Override
        @SuppressWarnings({ "unchecked", "rawtypes" })
        public <E> void onListFieldReplace(Transaction tx, ObjId id,
          ListField<E> field, int[] path, NavigableSet<ObjId> referrers, int index, E oldValue, E newValue) {
            final PermazenTransaction ptx = (PermazenTransaction)tx.getUserObject();
            assert ptx != null && ptx.tx == tx;
            final PermazenListField pfield = this.getField(ptx, id, field, PermazenListField.class);
            if (pfield == null)
                return;
            final Object poldValue = this.convertCoreValue(ptx, pfield.elementField, oldValue);
            final Object pnewValue = this.convertCoreValue(ptx, pfield.elementField, newValue);
            final PermazenObject pobj = this.checkTypes(ptx, ListFieldReplace.class, id, poldValue, pnewValue);
            if (pobj == null)
                return;
            this.invoke(ptx, referrers, new ListFieldReplace(pobj, pfield.name, index, poldValue, pnewValue));
        }

        @Override
        public void onListFieldClear(Transaction tx, ObjId id, ListField<?> field, int[] path, NavigableSet<ObjId> referrers) {
            final PermazenTransaction ptx = (PermazenTransaction)tx.getUserObject();
            assert ptx != null && ptx.tx == tx;
            final PermazenListField pfield = this.getField(ptx, id, field, PermazenListField.class);
            if (pfield == null)
                return;
            final PermazenObject pobj = this.checkTypes(ptx, ListFieldClear.class, id);
            if (pobj == null)
                return;
            this.invoke(ptx, referrers, new ListFieldClear<>(pobj, pfield.name));
        }

    // MapFieldChangeListener

        @Override
        @SuppressWarnings({ "unchecked", "rawtypes" })
        public <K, V> void onMapFieldAdd(Transaction tx, ObjId id,
          MapField<K, V> field, int[] path, NavigableSet<ObjId> referrers, K key, V value) {
            final PermazenTransaction ptx = (PermazenTransaction)tx.getUserObject();
            assert ptx != null && ptx.tx == tx;
            final PermazenMapField pfield = this.getField(ptx, id, field, PermazenMapField.class);
            if (pfield == null)
                return;
            final Object pkey = this.convertCoreValue(ptx, pfield.keyField, key);
            final Object pvalue = this.convertCoreValue(ptx, pfield.valueField, value);
            final PermazenObject pobj = this.checkTypes(ptx, MapFieldAdd.class, id, pkey, pvalue);
            if (pobj == null)
                return;
            this.invoke(ptx, referrers, new MapFieldAdd(pobj, pfield.name, pkey, pvalue));
        }

        @Override
        @SuppressWarnings({ "unchecked", "rawtypes" })
        public <K, V> void onMapFieldRemove(Transaction tx, ObjId id,
          MapField<K, V> field, int[] path, NavigableSet<ObjId> referrers, K key, V value) {
            final PermazenTransaction ptx = (PermazenTransaction)tx.getUserObject();
            assert ptx != null && ptx.tx == tx;
            final PermazenMapField pfield = this.getField(ptx, id, field, PermazenMapField.class);
            if (pfield == null)
                return;
            final Object pkey = this.convertCoreValue(ptx, pfield.keyField, key);
            final Object pvalue = this.convertCoreValue(ptx, pfield.valueField, value);
            final PermazenObject pobj = this.checkTypes(ptx, MapFieldRemove.class, id, pkey, pvalue);
            if (pobj == null)
                return;
            this.invoke(ptx, referrers, new MapFieldRemove(pobj, pfield.name, pkey, pvalue));
        }

        @Override
        @SuppressWarnings({ "unchecked", "rawtypes" })
        public <K, V> void onMapFieldReplace(Transaction tx, ObjId id,
          MapField<K, V> field, int[] path, NavigableSet<ObjId> referrers, K key, V oldValue, V newValue) {
            final PermazenTransaction ptx = (PermazenTransaction)tx.getUserObject();
            assert ptx != null && ptx.tx == tx;
            final PermazenMapField pfield = this.getField(ptx, id, field, PermazenMapField.class);
            if (pfield == null)
                return;
            final Object pkey = this.convertCoreValue(ptx, pfield.keyField, key);
            final Object poldValue = this.convertCoreValue(ptx, pfield.valueField, oldValue);
            final Object pnewValue = this.convertCoreValue(ptx, pfield.valueField, newValue);
            final PermazenObject pobj = this.checkTypes(ptx, MapFieldReplace.class, id, pkey, poldValue, pnewValue);
            if (pobj == null)
                return;
            this.invoke(ptx, referrers,
              new MapFieldReplace(pobj, pfield.name, pkey, poldValue, pnewValue));
        }

        @Override
        public void onMapFieldClear(Transaction tx, ObjId id, MapField<?, ?> field, int[] path, NavigableSet<ObjId> referrers) {
            final PermazenTransaction ptx = (PermazenTransaction)tx.getUserObject();
            assert ptx != null && ptx.tx == tx;
            final PermazenMapField pfield = this.getField(ptx, id, field, PermazenMapField.class);
            if (pfield == null)
                return;
            final PermazenObject pobj = this.checkTypes(ptx, MapFieldClear.class, id);
            if (pobj == null)
                return;
            this.invoke(ptx, referrers, new MapFieldClear<>(pobj, pfield.name));
        }

    // Internal methods

        private <T extends PermazenField> T getField(PermazenTransaction ptx, ObjId id, Field<?> field, Class<T> type) {
            try {
                return ptx.pdb.getField(id, field.getName(), type);
            } catch (TypeNotInSchemaException | UnknownFieldException e) {
                return null;        // somebody changed the field directly via the core API without first upgrading the object
            }
        }

        @SuppressWarnings({ "rawtypes", "unchecked" })
        private Object convertCoreValue(PermazenTransaction ptx, PermazenSimpleField pfield, Object value) {
            final Converter converter = pfield.getConverter(ptx);
            return converter != null ? converter.convert(value) : value;
        }

        private PermazenObject checkTypes(PermazenTransaction ptx,
          Class<? /*extends FieldChange<?>*/> changeType, ObjId id, Object... values) {

            // Check method parameter type
            final Method method = this.getMethod();
            if (!method.getParameterTypes()[0].isAssignableFrom(changeType))
                return null;

            // Check first generic type parameter which is the PermazenObject corresponding to id
            final PermazenObject pobj = ptx.get(id);
            if (!this.genericTypes[0].isInstance(pobj))
                return null;

            // Check other generic type parameter(s)
            for (int i = 1; i < this.genericTypes.length; i++) {
                final Object value = values[Math.min(i, values.length) - 1];
                if (value != null && !this.genericTypes[i].isInstance(value))
                    return null;
            }

            // OK types agree
            return pobj;
        }

        // Invoke the @OnChange method
        private void invoke(PermazenTransaction ptx, NavigableSet<ObjId> referrers, FieldChange<PermazenObject> change) {
            assert change != null;
            final Method method = this.getMethod();
            if ((method.getModifiers() & Modifier.STATIC) != 0)
                Util.invoke(method, null, change);
            else {
                for (ObjId id : referrers) {
                    final PermazenObject target = ptx.get(id);             // type of 'id' should always be found

                    // Avoid invoking subclass's @OnChange method on superclass instance;
                    // this can happen when the field is in superclass but wildcard @OnChange is in the subclass
                    if (method.getDeclaringClass().isInstance(target))
                        Util.invoke(method, target, change);
                }
            }
        }
    }
}
