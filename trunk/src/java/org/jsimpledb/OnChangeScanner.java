
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
import org.jsimpledb.core.ObjId;
import org.jsimpledb.core.Transaction;

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

    class ChangeMethodInfo extends MethodInfo implements AllChangesListener {

        final boolean isStatic;
        final List<ReferencePath> paths;
        final Class<? extends FieldChange<T>> rawParameterType;

        @SuppressWarnings("unchecked")
        ChangeMethodInfo(Method method, OnChange annotation) {
            super(method, annotation);

            // Get start type
            this.isStatic = (method.getModifiers() & Modifier.STATIC) != 0;
            TypeToken<?> startType = TypeToken.of(method.getDeclaringClass());
            if (annotation.startType() != void.class) {
                if (!this.isStatic) {
                    throw new IllegalArgumentException(OnChangeScanner.this.getErrorPrefix(method)
                      + "startType() may only be used for annotations on static methods");
                }
                if (annotation.startType().isPrimitive() || annotation.startType().isArray()) {
                    throw new IllegalArgumentException(OnChangeScanner.this.getErrorPrefix(method)
                      + "invalid startType() " + annotation.startType());
                }
                startType = TypeToken.of(annotation.startType());
            }

            // Replace empty reference path list with "all fields in this object"
            List<String> stringPaths = Arrays.asList(annotation.value());
            final boolean wildcard = stringPaths.isEmpty();
            if (wildcard) {
                stringPaths = Lists.transform(Lists.newArrayList(OnChangeScanner.this.jclass.jfields.values()),
                  new Function<JField, String>() {
                    @Override
                    public String apply(JField jfield) {
                        return jfield.name;
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
                    path.targetField.addChangeParameterTypes(changeParameterTypes, path.targetType);
                } catch (UnsupportedOperationException e) {
                    if (wildcard)
                        continue;
                    throw new IllegalArgumentException(OnChangeScanner.this.getErrorPrefix(method) + "path `"
                      + stringPath + "' is invalid because change notifications are not supported for " + path.targetField, e);
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
                      + "' is invalid because no changes emitted by " + path.targetField + " match the method's"
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
        void registerChangeListener(Transaction tx) {
            for (ReferencePath path : this.paths)
                path.targetField.registerChangeListener(tx, path.getReferenceFields(), this);
        }

        private Object convert(JField jfield, int index, Object obj) {
            if (obj == null)
                return null;
            boolean isReference;
            if (index == -1) {
                assert jfield instanceof JSimpleField;
                isReference = jfield instanceof JReferenceField;
            } else {
                assert jfield instanceof JComplexField;
                isReference = ((JComplexField)jfield).getSubFields().get(index) instanceof JReferenceField;
            }
            return isReference ? this.getJObject((ObjId)obj) : obj;
        }

    // SimpleFieldChangeListener

        @Override
        @SuppressWarnings({ "unchecked", "rawtypes" })
        public <T> void onSimpleFieldChange(Transaction tx, ObjId id,
          int storageId, int[] path, NavigableSet<ObjId> referrers, T oldValue, T newValue) {
            if (!this.rawParameterType.isAssignableFrom(SimpleFieldChange.class))
                return;
            final JField jfield = this.getJField(storageId);
            this.invoke(referrers, new SimpleFieldChange(this.getJObject(id),
              jfield.name, this.convert(jfield, -1, oldValue), this.convert(jfield, -1, newValue)));
        }

    // SetFieldChangeListener

        @Override
        @SuppressWarnings({ "unchecked", "rawtypes" })
        public <E> void onSetFieldAdd(Transaction tx, ObjId id, int storageId, int[] path, NavigableSet<ObjId> referrers, E value) {
            if (!this.rawParameterType.isAssignableFrom(SetFieldAdd.class))
                return;
            final JField jfield = this.getJField(storageId);
            this.invoke(referrers, new SetFieldAdd(this.getJObject(id), jfield.name, this.convert(jfield, 0, value)));
        }

        @Override
        @SuppressWarnings({ "unchecked", "rawtypes" })
        public <E> void onSetFieldRemove(Transaction tx, ObjId id, int storageId, int[] path,
          NavigableSet<ObjId> referrers, E value) {
            if (!this.rawParameterType.isAssignableFrom(SetFieldRemove.class))
                return;
            final JField jfield = this.getJField(storageId);
            this.invoke(referrers, new SetFieldRemove(this.getJObject(id),
              jfield.name, this.convert(jfield, 0, value)));
        }

        @Override
        public void onSetFieldClear(Transaction tx, ObjId id, int storageId, int[] path, NavigableSet<ObjId> referrers) {
            if (!this.rawParameterType.isAssignableFrom(SetFieldClear.class))
                return;
            final JField jfield = this.getJField(storageId);
            this.invoke(referrers, new SetFieldClear<JObject>(this.getJObject(id), jfield.name));
        }

    // ListFieldChangeListener

        @Override
        @SuppressWarnings({ "unchecked", "rawtypes" })
        public <E> void onListFieldAdd(Transaction tx, ObjId id, int storageId, int[] path,
          NavigableSet<ObjId> referrers, int index, E value) {
            if (!this.rawParameterType.isAssignableFrom(ListFieldAdd.class))
                return;
            final JField jfield = this.getJField(storageId);
            this.invoke(referrers, new ListFieldAdd(this.getJObject(id),
              jfield.name, index, this.convert(jfield, 0, value)));
        }

        @Override
        @SuppressWarnings({ "unchecked", "rawtypes" })
        public <E> void onListFieldRemove(Transaction tx, ObjId id, int storageId, int[] path,
          NavigableSet<ObjId> referrers, int index, E value) {
            if (!this.rawParameterType.isAssignableFrom(ListFieldRemove.class))
                return;
            final JField jfield = this.getJField(storageId);
            this.invoke(referrers, new ListFieldRemove(this.getJObject(id),
              jfield.name, index, this.convert(jfield, 0, value)));
        }

        @Override
        @SuppressWarnings({ "unchecked", "rawtypes" })
        public <E> void onListFieldReplace(Transaction tx, ObjId id, int storageId, int[] path,
          NavigableSet<ObjId> referrers, int index, E oldValue, E newValue) {
            if (!this.rawParameterType.isAssignableFrom(ListFieldReplace.class))
                return;
            final JField jfield = this.getJField(storageId);
            this.invoke(referrers, new ListFieldReplace(this.getJObject(id),
              jfield.name, index, this.convert(jfield, 0, oldValue), this.convert(jfield, 0, newValue)));
        }

        @Override
        public void onListFieldClear(Transaction tx, ObjId id, int storageId, int[] path, NavigableSet<ObjId> referrers) {
            if (!this.rawParameterType.isAssignableFrom(ListFieldClear.class))
                return;
            final JField jfield = this.getJField(storageId);
            this.invoke(referrers, new ListFieldClear<JObject>(this.getJObject(id), jfield.name));
        }

    // MapFieldChangeListener

        @Override
        @SuppressWarnings({ "unchecked", "rawtypes" })
        public <K, V> void onMapFieldAdd(Transaction tx, ObjId id, int storageId, int[] path, NavigableSet<ObjId> referrers,
          K key, V value) {
            if (!this.rawParameterType.isAssignableFrom(MapFieldAdd.class))
                return;
            final JField jfield = this.getJField(storageId);
            this.invoke(referrers, new MapFieldAdd(this.getJObject(id),
              jfield.name, this.convert(jfield, 0, key), this.convert(jfield, 1, value)));
        }

        @Override
        @SuppressWarnings({ "unchecked", "rawtypes" })
        public <K, V> void onMapFieldRemove(Transaction tx, ObjId id, int storageId, int[] path, NavigableSet<ObjId> referrers,
          K key, V value) {
            if (!this.rawParameterType.isAssignableFrom(MapFieldRemove.class))
                return;
            final JField jfield = this.getJField(storageId);
            this.invoke(referrers, new MapFieldRemove(this.getJObject(id),
              jfield.name, this.convert(jfield, 0, key), this.convert(jfield, 1, value)));
        }

        @Override
        @SuppressWarnings({ "unchecked", "rawtypes" })
        public <K, V> void onMapFieldReplace(Transaction tx, ObjId id, int storageId, int[] path, NavigableSet<ObjId> referrers,
          K key, V oldValue, V newValue) {
            if (!this.rawParameterType.isAssignableFrom(MapFieldReplace.class))
                return;
            final JField jfield = this.getJField(storageId);
            this.invoke(referrers, new MapFieldReplace(this.getJObject(id),
              jfield.name, this.convert(jfield, 0, key), this.convert(jfield, 1, oldValue), this.convert(jfield, 1, newValue)));
        }

        @Override
        public void onMapFieldClear(Transaction tx, ObjId id, int storageId, int[] path, NavigableSet<ObjId> referrers) {
            if (!this.rawParameterType.isAssignableFrom(MapFieldClear.class))
                return;
            final JField jfield = this.getJField(storageId);
            this.invoke(referrers, new MapFieldClear<JObject>(this.getJObject(id), jfield.name));
        }

    // Internal methods

        private JObject getJObject(ObjId id) {
            return JTransaction.getCurrent().jdb.getJObject(id);
        }

        private JField getJField(int storageId) {
            return OnChangeScanner.this.jclass.jdb.jfields.get(storageId);
        }

        private void invoke(NavigableSet<ObjId> referrers, FieldChange<JObject> change) {
            if (this.isStatic)
                this.invoke(null, change);
            else {
                final JSimpleDB jdb = JTransaction.getCurrent().jdb;
                for (ObjId id : referrers)
                    this.invoke(jdb.getJObject(id), change);
            }
        }

        private void invoke(JObject target, FieldChange<JObject> change) {

            // Avoid invoking subclass's @OnChange method on superclass instance (can happen when field is in superclass)
            if (!this.getMethod().getDeclaringClass().isInstance(target))
                return;

            // Invoke method
            Util.invoke(this.getMethod(), target, change);
        }
    }
}

