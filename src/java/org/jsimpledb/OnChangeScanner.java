
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.reflect.TypeToken;

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;

import org.jsimpledb.annotation.OnChange;
import org.jsimpledb.change.FieldChange;
import org.jsimpledb.change.ListFieldAdd;
import org.jsimpledb.change.ListFieldClear;
import org.jsimpledb.change.ListFieldRemove;
import org.jsimpledb.change.ListFieldReplace;
import org.jsimpledb.change.MapFieldAdd;
import org.jsimpledb.change.MapFieldClear;
import org.jsimpledb.change.MapFieldRemove;
import org.jsimpledb.change.MapFieldReplace;
import org.jsimpledb.change.SetFieldAdd;
import org.jsimpledb.change.SetFieldClear;
import org.jsimpledb.change.SetFieldRemove;
import org.jsimpledb.change.SimpleFieldChange;
import org.jsimpledb.core.ListField;
import org.jsimpledb.core.MapField;
import org.jsimpledb.core.ObjId;
import org.jsimpledb.core.SetField;
import org.jsimpledb.core.SimpleField;
import org.jsimpledb.core.Transaction;
import org.jsimpledb.util.AnnotationScanner;

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
        if (this.getParameterTypeTokens(method).size() != 1)
            throw new IllegalArgumentException(this.getErrorPrefix(method) + "method is required to take a single parameter");
        return true;                                    // we do further parameter type check in ChangeMethodInfo
    }

    @Override
    protected ChangeMethodInfo createMethodInfo(Method method, OnChange annotation) {
        return new ChangeMethodInfo(method, annotation);
    }

// ChangeMethodInfo

    class ChangeMethodInfo extends MethodInfo {

        final boolean isStatic;
        final HashMap<ReferencePath, HashSet<Integer>> paths;
        final Class<? extends FieldChange<T>> rawParameterType;

        @SuppressWarnings("unchecked")
        ChangeMethodInfo(Method method, OnChange annotation) {
            super(method, annotation);

            // Get start type
            this.isStatic = (method.getModifiers() & Modifier.STATIC) != 0;
            Class<?> startType = method.getDeclaringClass();
            if (annotation.startType() != void.class) {
                if (!this.isStatic) {
                    throw new IllegalArgumentException(OnChangeScanner.this.getErrorPrefix(method)
                      + "startType() may only be used for annotations on static methods");
                }
                if (annotation.startType().isPrimitive() || annotation.startType().isArray()) {
                    throw new IllegalArgumentException(OnChangeScanner.this.getErrorPrefix(method)
                      + "invalid startType() " + annotation.startType());
                }
                startType = annotation.startType();
            }

            // Replace empty reference path list with "all fields in this object"
            List<String> stringPaths = Arrays.asList(annotation.value());
            final boolean wildcard = stringPaths.isEmpty();
            if (wildcard) {
                stringPaths = Lists.transform(Lists.newArrayList(OnChangeScanner.this.jclass.jfields.values()),
                  new Function<JField, String>() {
                    @Override
                    public String apply(JField jfield) {
                        return jfield.name + "#" + jfield.storageId;
                    }
                });
            }

            // Get method parameter type (generic and raw)
            final TypeToken<?> genericParameterType = OnChangeScanner.this.getParameterTypeTokens(method).get(0);
            this.rawParameterType = (Class<? extends FieldChange<T>>)method.getParameterTypes()[0];

            // Parse reference paths
            boolean anyFieldsFound = false;
            this.paths = new HashMap<ReferencePath, HashSet<Integer>>(stringPaths.size());
            for (int i = 0; i < stringPaths.size(); i++) {
                final String stringPath = stringPaths.get(i);

                // Parse reference path
                final ReferencePath path;
                try {
                    path = OnChangeScanner.this.jclass.jdb.parseReferencePath(startType, stringPath, false);
                } catch (IllegalArgumentException e) {
                    throw new IllegalArgumentException(OnChangeScanner.this.getErrorPrefix(method) + e.getMessage(), e);
                }

                // Get all (concrete) change types emitted by the target field
                final ArrayList<TypeToken<?>> changeParameterTypes = new ArrayList<TypeToken<?>>();
                try {
                    path.targetFieldInfo.addChangeParameterTypes(changeParameterTypes, path.targetType);
                } catch (UnsupportedOperationException e) {
                    if (wildcard)
                        continue;
                    throw new IllegalArgumentException(OnChangeScanner.this.getErrorPrefix(method) + "path `"
                      + stringPath + "' is invalid because change notifications are not supported for " + path.targetFieldInfo, e);
                }
                anyFieldsFound = true;

                // Check whether method parameter type accepts as least one of them; must do so consistently raw vs. generic
                boolean anyChangeMatch = false;
                for (TypeToken<?> possibleChangeType : changeParameterTypes) {
                    final boolean matchesGeneric = genericParameterType.isAssignableFrom(possibleChangeType);
                    final boolean matchesRaw = rawParameterType.isAssignableFrom(possibleChangeType.getRawType());
                    if (matchesGeneric != matchesRaw) {
                        throw new IllegalArgumentException(OnChangeScanner.this.getErrorPrefix(method) + "method parameter type "
                          + genericParameterType + " will match changes emitted from `" + stringPath + "' at runtime"
                          + " due to type erasure, but has incompatible generic type " + genericParameterType
                          + "; parameter type should be compatible with "
                          + (changeParameterTypes.size() != 1 ? "one of: " + changeParameterTypes : changeParameterTypes.get(0)));
                    }
                    if (matchesGeneric) {
                        anyChangeMatch = true;
                        break;
                    }
                }

                // If not wildcard match, then at least one change type must match method
                if (!anyChangeMatch) {
                    if (wildcard)
                        continue;
                    throw new IllegalArgumentException(OnChangeScanner.this.getErrorPrefix(method) + "path `" + stringPath
                      + "' is invalid because no changes emitted by " + path.targetFieldInfo + " match the method's"
                      + " parameter type " + genericParameterType + "; the emitted change type is "
                      + (changeParameterTypes.size() != 1 ? "one of: " + changeParameterTypes : changeParameterTypes.get(0)));
                }

                // Determine storage ID's corresponding to matching target types; this filters out obsolete types from old versions
                final HashSet<Integer> storageIds = new HashSet<Integer>();
                for (JClass<?> jclass : OnChangeScanner.this.jclass.jdb.getJClasses(path.targetType))
                    storageIds.add(jclass.storageId);

                // Match
                this.paths.put(path, storageIds);
            }

            // No matching fields?
            if (this.paths.isEmpty()) {                                                         // must be wildcard
                if (!anyFieldsFound) {
                    throw new IllegalArgumentException(OnChangeScanner.this.getErrorPrefix(method)
                      + "there are no fields that will generate change events");
                }
                throw new IllegalArgumentException(OnChangeScanner.this.getErrorPrefix(method) + "no changes emitted by any field"
                  + " will match the method's parameter type " + genericParameterType);
            }
        }

        // Register listeners for this method
        void registerChangeListener(JTransaction jtx) {
            final ChangeMethodListener listener = new ChangeMethodListener(jtx, this.getMethod());
            for (Map.Entry<ReferencePath, HashSet<Integer>> entry : this.paths.entrySet()) {
                final ReferencePath path = entry.getKey();
                final HashSet<Integer> objectTypeStorageIds = entry.getValue();
                path.targetFieldInfo.registerChangeListener(jtx.tx, path.getReferenceFields(), objectTypeStorageIds, listener);
            }
        }
    }

// ChangeMethodListener

    static class ChangeMethodListener implements AllChangesListener {

        private final JTransaction jtx;
        private final Method method;
        private final Class<?>[] genericTypes;

        ChangeMethodListener(JTransaction jtx, Method method) {
            Preconditions.checkArgument(jtx != null, "null jtx");
            Preconditions.checkArgument(method != null, "null method");
            this.jtx = jtx;
            this.method = method;

            // Extract generic types from method's FieldChange<?> parameter
            final ArrayList<Class<?>> genericTypeList = new ArrayList<>(3);
            for (Type type : ((ParameterizedType)this.method.getGenericParameterTypes()[0]).getActualTypeArguments())
                genericTypeList.add(TypeToken.of(type).getRawType());
            this.genericTypes = genericTypeList.toArray(new Class<?>[genericTypeList.size()]);
        }

    // SimpleFieldChangeListener

        @Override
        @SuppressWarnings({ "unchecked", "rawtypes" })
        public <T> void onSimpleFieldChange(Transaction tx, ObjId id,
          SimpleField<T> field, int[] path, NavigableSet<ObjId> referrers, T oldValue, T newValue) {
            final Object joldValue = this.jtx.convertCoreValue(field, oldValue);
            final Object jnewValue = this.jtx.convertCoreValue(field, newValue);
            final JObject jobj = this.checkTypes(SimpleFieldChange.class, id, joldValue, jnewValue);
            if (jobj == null)
                return;
            this.invoke(referrers, new SimpleFieldChange(jobj, field.getStorageId(), field.getName(), joldValue, jnewValue));
        }

    // SetFieldChangeListener

        @Override
        @SuppressWarnings({ "unchecked", "rawtypes" })
        public <E> void onSetFieldAdd(Transaction tx, ObjId id,
          SetField<E> field, int[] path, NavigableSet<ObjId> referrers, E value) {
            final Object jvalue = this.jtx.convertCoreValue(field.getElementField(), value);
            final JObject jobj = this.checkTypes(SetFieldAdd.class, id, jvalue);
            if (jobj == null)
                return;
            this.invoke(referrers, new SetFieldAdd(jobj, field.getStorageId(), field.getName(), jvalue));
        }

        @Override
        @SuppressWarnings({ "unchecked", "rawtypes" })
        public <E> void onSetFieldRemove(Transaction tx, ObjId id,
          SetField<E> field, int[] path, NavigableSet<ObjId> referrers, E value) {
            final Object jvalue = this.jtx.convertCoreValue(field.getElementField(), value);
            final JObject jobj = this.checkTypes(SetFieldRemove.class, id, jvalue);
            if (jobj == null)
                return;
            this.invoke(referrers, new SetFieldRemove(jobj, field.getStorageId(), field.getName(), jvalue));
        }

        @Override
        public void onSetFieldClear(Transaction tx, ObjId id, SetField<?> field, int[] path, NavigableSet<ObjId> referrers) {
            final JObject jobj = this.checkTypes(SetFieldClear.class, id);
            if (jobj == null)
                return;
            this.invoke(referrers, new SetFieldClear<JObject>(jobj, field.getStorageId(), field.getName()));
        }

    // ListFieldChangeListener

        @Override
        @SuppressWarnings({ "unchecked", "rawtypes" })
        public <E> void onListFieldAdd(Transaction tx, ObjId id,
          ListField<E> field, int[] path, NavigableSet<ObjId> referrers, int index, E value) {
            final Object jvalue = this.jtx.convertCoreValue(field.getElementField(), value);
            final JObject jobj = this.checkTypes(ListFieldAdd.class, id, jvalue);
            if (jobj == null)
                return;
            this.invoke(referrers, new ListFieldAdd(jobj, field.getStorageId(), field.getName(), index, jvalue));
        }

        @Override
        @SuppressWarnings({ "unchecked", "rawtypes" })
        public <E> void onListFieldRemove(Transaction tx, ObjId id,
          ListField<E> field, int[] path, NavigableSet<ObjId> referrers, int index, E value) {
            final Object jvalue = this.jtx.convertCoreValue(field.getElementField(), value);
            final JObject jobj = this.checkTypes(ListFieldRemove.class, id, jvalue);
            if (jobj == null)
                return;
            this.invoke(referrers, new ListFieldRemove(jobj, field.getStorageId(), field.getName(), index, jvalue));
        }

        @Override
        @SuppressWarnings({ "unchecked", "rawtypes" })
        public <E> void onListFieldReplace(Transaction tx, ObjId id,
          ListField<E> field, int[] path, NavigableSet<ObjId> referrers, int index, E oldValue, E newValue) {
            final Object joldValue = this.jtx.convertCoreValue(field.getElementField(), oldValue);
            final Object jnewValue = this.jtx.convertCoreValue(field.getElementField(), newValue);
            final JObject jobj = this.checkTypes(ListFieldReplace.class, id, joldValue, jnewValue);
            if (jobj == null)
                return;
            this.invoke(referrers, new ListFieldReplace(jobj, field.getStorageId(), field.getName(), index, joldValue, jnewValue));
        }

        @Override
        public void onListFieldClear(Transaction tx, ObjId id, ListField<?> field, int[] path, NavigableSet<ObjId> referrers) {
            final JObject jobj = this.checkTypes(ListFieldClear.class, id);
            if (jobj == null)
                return;
            this.invoke(referrers, new ListFieldClear<JObject>(jobj, field.getStorageId(), field.getName()));
        }

    // MapFieldChangeListener

        @Override
        @SuppressWarnings({ "unchecked", "rawtypes" })
        public <K, V> void onMapFieldAdd(Transaction tx, ObjId id,
          MapField<K, V> field, int[] path, NavigableSet<ObjId> referrers, K key, V value) {
            final Object jkey = this.jtx.convertCoreValue(field.getKeyField(), key);
            final Object jvalue = this.jtx.convertCoreValue(field.getValueField(), value);
            final JObject jobj = this.checkTypes(MapFieldAdd.class, id, jkey, jvalue);
            if (jobj == null)
                return;
            this.invoke(referrers, new MapFieldAdd(jobj, field.getStorageId(), field.getName(), jkey, jvalue));
        }

        @Override
        @SuppressWarnings({ "unchecked", "rawtypes" })
        public <K, V> void onMapFieldRemove(Transaction tx, ObjId id,
          MapField<K, V> field, int[] path, NavigableSet<ObjId> referrers, K key, V value) {
            final Object jkey = this.jtx.convertCoreValue(field.getKeyField(), key);
            final Object jvalue = this.jtx.convertCoreValue(field.getValueField(), value);
            final JObject jobj = this.checkTypes(MapFieldRemove.class, id, jkey, jvalue);
            if (jobj == null)
                return;
            this.invoke(referrers, new MapFieldRemove(jobj, field.getStorageId(), field.getName(), jkey, jvalue));
        }

        @Override
        @SuppressWarnings({ "unchecked", "rawtypes" })
        public <K, V> void onMapFieldReplace(Transaction tx, ObjId id,
          MapField<K, V> field, int[] path, NavigableSet<ObjId> referrers, K key, V oldValue, V newValue) {
            final Object jkey = this.jtx.convertCoreValue(field.getKeyField(), key);
            final Object joldValue = this.jtx.convertCoreValue(field.getValueField(), oldValue);
            final Object jnewValue = this.jtx.convertCoreValue(field.getValueField(), newValue);
            final JObject jobj = this.checkTypes(MapFieldReplace.class, id, jkey, joldValue, jnewValue);
            if (jobj == null)
                return;
            this.invoke(referrers, new MapFieldReplace(jobj, field.getStorageId(), field.getName(), jkey, joldValue, jnewValue));
        }

        @Override
        public void onMapFieldClear(Transaction tx, ObjId id, MapField<?, ?> field, int[] path, NavigableSet<ObjId> referrers) {
            final JObject jobj = this.checkTypes(MapFieldClear.class, id);
            if (jobj == null)
                return;
            this.invoke(referrers, new MapFieldClear<JObject>(jobj, field.getStorageId(), field.getName()));
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == this)
                return true;
            if (obj == null || obj.getClass() != this.getClass())
                return false;
            final ChangeMethodListener that = (ChangeMethodListener)obj;
            return this.method.equals(that.method) && this.jtx.equals(that.jtx);
        }

        @Override
        public int hashCode() {
            return this.method.hashCode() ^ this.jtx.hashCode();
        }

    // Internal methods

        private JObject checkTypes(Class<? /*extends FieldChange<?>*/> changeType, ObjId id, Object... values) {

            // Check method parameter type
            if (!method.getParameterTypes()[0].isAssignableFrom(changeType))
                return null;

            // Check first generic type parameter which is the JObject corresponding to id
            final JObject jobj = this.jtx.getJObject(id);
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

        private void invoke(NavigableSet<ObjId> referrers, FieldChange<JObject> change) {
            if ((this.method.getModifiers() & Modifier.STATIC) != 0)
                Util.invoke(this.method, null, change);
            else {
                for (ObjId id : referrers) {
                    final JObject target = this.jtx.getJObject(id);     // type of 'id' should always be found

                    // Avoid invoking subclass's @OnChange method on superclass instance;
                    // this can happen when the field is in superclass but wildcard @OnChange is in the subclass
                    if (this.method.getDeclaringClass().isInstance(target))
                        Util.invoke(this.method, target, change);
                }
            }
        }
    }
}

