
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb;

import com.google.common.base.Function;
import com.google.common.collect.Lists;
import com.google.common.reflect.TypeToken;

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
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
        final List<ReferencePath> paths;
        final Class<? extends FieldChange<T>> rawParameterType;

        @SuppressWarnings("unchecked")
        ChangeMethodInfo(Method method, OnChange annotation) {
            super(method, annotation);

            // Get start type
            this.isStatic = (method.getModifiers() & Modifier.STATIC) != 0;
            TypeToken<?> startType = Util.getWildcardedType(method.getDeclaringClass());
            if (annotation.startType() != void.class) {
                if (!this.isStatic) {
                    throw new IllegalArgumentException(OnChangeScanner.this.getErrorPrefix(method)
                      + "startType() may only be used for annotations on static methods");
                }
                if (annotation.startType().isPrimitive() || annotation.startType().isArray()) {
                    throw new IllegalArgumentException(OnChangeScanner.this.getErrorPrefix(method)
                      + "invalid startType() " + annotation.startType());
                }
                startType = Util.getWildcardedType(annotation.startType());
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
            this.paths = new ArrayList<ReferencePath>(stringPaths.size());
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

                // Match
                this.paths.add(path);
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
            for (ReferencePath path : this.paths)
                path.targetFieldInfo.registerChangeListener(jtx.tx, path.getReferenceFields(), listener);
        }
    }

// ChangeMethodListener

    static class ChangeMethodListener implements AllChangesListener {

        private final JTransaction jtx;
        private final Method method;

        ChangeMethodListener(JTransaction jtx, Method method) {
            if (jtx == null)
                throw new IllegalArgumentException("null jtx");
            if (method == null)
                throw new IllegalArgumentException("null method");
            this.jtx = jtx;
            this.method = method;
        }

    // SimpleFieldChangeListener

        @Override
        @SuppressWarnings({ "unchecked", "rawtypes" })
        public <T> void onSimpleFieldChange(Transaction tx, ObjId id,
          SimpleField<T> field, int[] path, NavigableSet<ObjId> referrers, T oldValue, T newValue) {
            if (!this.canInvokeWith(SimpleFieldChange.class))
                return;
            final JObject jobj = this.getJObject(id);
            if (jobj == null)
                return;
            this.invoke(referrers, new SimpleFieldChange(jobj, field.getStorageId(), field.getName(),
              this.jtx.convertCoreValue(field, oldValue), this.jtx.convertCoreValue(field, newValue)));
        }

    // SetFieldChangeListener

        @Override
        @SuppressWarnings({ "unchecked", "rawtypes" })
        public <E> void onSetFieldAdd(Transaction tx, ObjId id,
          SetField<E> field, int[] path, NavigableSet<ObjId> referrers, E value) {
            if (!this.canInvokeWith(SetFieldAdd.class))
                return;
            final JObject jobj = this.getJObject(id);
            if (jobj == null)
                return;
            this.invoke(referrers, new SetFieldAdd(jobj, field.getStorageId(),
              field.getName(), this.jtx.convertCoreValue(field.getElementField(), value)));
        }

        @Override
        @SuppressWarnings({ "unchecked", "rawtypes" })
        public <E> void onSetFieldRemove(Transaction tx, ObjId id,
          SetField<E> field, int[] path, NavigableSet<ObjId> referrers, E value) {
            if (!this.canInvokeWith(SetFieldRemove.class))
                return;
            final JObject jobj = this.getJObject(id);
            if (jobj == null)
                return;
            this.invoke(referrers, new SetFieldRemove(jobj, field.getStorageId(),
              field.getName(), this.jtx.convertCoreValue(field.getElementField(), value)));
        }

        @Override
        public void onSetFieldClear(Transaction tx, ObjId id, SetField<?> field, int[] path, NavigableSet<ObjId> referrers) {
            if (!this.canInvokeWith(SetFieldClear.class))
                return;
            final JObject jobj = this.getJObject(id);
            if (jobj == null)
                return;
            this.invoke(referrers, new SetFieldClear<JObject>(jobj, field.getStorageId(), field.getName()));
        }

    // ListFieldChangeListener

        @Override
        @SuppressWarnings({ "unchecked", "rawtypes" })
        public <E> void onListFieldAdd(Transaction tx, ObjId id,
          ListField<E> field, int[] path, NavigableSet<ObjId> referrers, int index, E value) {
            if (!this.canInvokeWith(ListFieldAdd.class))
                return;
            final JObject jobj = this.getJObject(id);
            if (jobj == null)
                return;
            this.invoke(referrers, new ListFieldAdd(jobj, field.getStorageId(),
              field.getName(), index, this.jtx.convertCoreValue(field.getElementField(), value)));
        }

        @Override
        @SuppressWarnings({ "unchecked", "rawtypes" })
        public <E> void onListFieldRemove(Transaction tx, ObjId id,
          ListField<E> field, int[] path, NavigableSet<ObjId> referrers, int index, E value) {
            if (!this.canInvokeWith(ListFieldRemove.class))
                return;
            final JObject jobj = this.getJObject(id);
            if (jobj == null)
                return;
            this.invoke(referrers, new ListFieldRemove(jobj, field.getStorageId(),
              field.getName(), index, this.jtx.convertCoreValue(field.getElementField(), value)));
        }

        @Override
        @SuppressWarnings({ "unchecked", "rawtypes" })
        public <E> void onListFieldReplace(Transaction tx, ObjId id,
          ListField<E> field, int[] path, NavigableSet<ObjId> referrers, int index, E oldValue, E newValue) {
            if (!this.canInvokeWith(ListFieldReplace.class))
                return;
            final JObject jobj = this.getJObject(id);
            if (jobj == null)
                return;
            this.invoke(referrers, new ListFieldReplace(jobj, field.getStorageId(), field.getName(), index,
              this.jtx.convertCoreValue(field.getElementField(), oldValue),
              this.jtx.convertCoreValue(field.getElementField(), newValue)));
        }

        @Override
        public void onListFieldClear(Transaction tx, ObjId id, ListField<?> field, int[] path, NavigableSet<ObjId> referrers) {
            if (!this.canInvokeWith(ListFieldClear.class))
                return;
            final JObject jobj = this.getJObject(id);
            if (jobj == null)
                return;
            this.invoke(referrers, new ListFieldClear<JObject>(jobj, field.getStorageId(), field.getName()));
        }

    // MapFieldChangeListener

        @Override
        @SuppressWarnings({ "unchecked", "rawtypes" })
        public <K, V> void onMapFieldAdd(Transaction tx, ObjId id,
          MapField<K, V> field, int[] path, NavigableSet<ObjId> referrers, K key, V value) {
            if (!this.canInvokeWith(MapFieldAdd.class))
                return;
            final JObject jobj = this.getJObject(id);
            if (jobj == null)
                return;
            this.invoke(referrers, new MapFieldAdd(jobj, field.getStorageId(), field.getName(),
              this.jtx.convertCoreValue(field.getKeyField(), key), this.jtx.convertCoreValue(field.getValueField(), value)));
        }

        @Override
        @SuppressWarnings({ "unchecked", "rawtypes" })
        public <K, V> void onMapFieldRemove(Transaction tx, ObjId id,
          MapField<K, V> field, int[] path, NavigableSet<ObjId> referrers, K key, V value) {
            if (!this.canInvokeWith(MapFieldRemove.class))
                return;
            final JObject jobj = this.getJObject(id);
            if (jobj == null)
                return;
            this.invoke(referrers, new MapFieldRemove(jobj, field.getStorageId(), field.getName(),
              this.jtx.convertCoreValue(field.getKeyField(), key), this.jtx.convertCoreValue(field.getValueField(), value)));
        }

        @Override
        @SuppressWarnings({ "unchecked", "rawtypes" })
        public <K, V> void onMapFieldReplace(Transaction tx, ObjId id,
          MapField<K, V> field, int[] path, NavigableSet<ObjId> referrers, K key, V oldValue, V newValue) {
            if (!this.canInvokeWith(MapFieldReplace.class))
                return;
            final JObject jobj = this.getJObject(id);
            if (jobj == null)
                return;
            this.invoke(referrers, new MapFieldReplace(jobj, field.getStorageId(), field.getName(),
              this.jtx.convertCoreValue(field.getKeyField(), key),
              this.jtx.convertCoreValue(field.getValueField(), oldValue),
              this.jtx.convertCoreValue(field.getValueField(), newValue)));
        }

        @Override
        public void onMapFieldClear(Transaction tx, ObjId id, MapField<?, ?> field, int[] path, NavigableSet<ObjId> referrers) {
            if (!this.canInvokeWith(MapFieldClear.class))
                return;
            final JObject jobj = this.getJObject(id);
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

        private JObject getJObject(ObjId id) {

            // If changed object is not representable via Java model class, don't propagate the change
            if (!this.jtx.jdb.jclasses.containsKey(id.getStorageId()))
                return null;

            // Get JObject
            return this.jtx.getJObject(id);
        }

        private boolean canInvokeWith(Class<?> paramType) {
            return this.method.getParameterTypes()[0].isAssignableFrom(paramType);
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

