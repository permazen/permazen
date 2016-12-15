
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb;

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
        if (this.getParameterTypeTokens(method).size() > 1)
            throw new IllegalArgumentException(this.getErrorPrefix(method) + "method is required to take zero or one parameter");
        return true;                                    // we do further parameter type check in ChangeMethodInfo
    }

    @Override
    protected ChangeMethodInfo createMethodInfo(Method method, OnChange annotation) {
        return new ChangeMethodInfo(method, annotation);
    }

// ChangeMethodInfo

    class ChangeMethodInfo extends MethodInfo implements AllChangesListener {

        final HashMap<ReferencePath, HashSet<Integer>> paths;
        final Class<?>[] genericTypes;      // derived from this.method, so there's no need to include it in equals() or hashCode()

        ChangeMethodInfo(Method method, OnChange annotation) {
            super(method, annotation);

            // Get database
            final JSimpleDB jdb = OnChangeScanner.this.jclass.jdb;

            // Get start type
            Class<?> startType = method.getDeclaringClass();
            if (annotation.startType() != void.class) {
                if ((method.getModifiers() & Modifier.STATIC) == 0) {
                    throw new IllegalArgumentException(OnChangeScanner.this.getErrorPrefix(method)
                      + "startType() may only be used for annotations on static methods");
                }
                if (annotation.startType().isPrimitive() || annotation.startType().isArray()) {
                    throw new IllegalArgumentException(OnChangeScanner.this.getErrorPrefix(method)
                      + "invalid startType() " + annotation.startType());
                }
                startType = annotation.startType();
            }

            // Initialize path list
            final List<String> unexpandedPathList = new ArrayList<>(Arrays.asList(annotation.value()));

            // An empty list is the same as @OnChange("*")
            if (unexpandedPathList.isEmpty())
                unexpandedPathList.add("*");

            // Replace paths ending in "*" them with an iteration of all fields in the corresponding type
            final List<String> expandedPathList = new ArrayList<>(unexpandedPathList.size());
            final HashSet<Integer> expandedPathWasWildcard = new HashSet<>();
            for (String unexpandedPath : unexpandedPathList) {

                // Check for immediate wildcard: "*"
                if (unexpandedPath.equals("*")) {

                    // Replace path with non-wildcard paths for every field in the start type
                    for (JClass<?> jclass : jdb.getJClasses(startType)) {
                        for (JField jfield : jclass.jfields.values()) {
                            expandedPathWasWildcard.add(expandedPathList.size());
                            expandedPathList.add(jfield.name + "#" + jfield.storageId);
                        }
                    }
                    continue;
                }

                // Check for reference path with wildcard: "foo.bar.*"
                if (unexpandedPath.length() > 2 && unexpandedPath.endsWith(".*")) {

                    // Parse the reference path up to the wildcard
                    final String prefixPath = unexpandedPath.substring(0, unexpandedPath.length() - 2);
                    final ReferencePath prefixReferencePath;
                    try {
                        prefixReferencePath = jdb.parseReferencePath(startType, prefixPath, true);
                    } catch (IllegalArgumentException e) {
                        throw new IllegalArgumentException(OnChangeScanner.this.getErrorPrefix(method) + e.getMessage(), e);
                    }

                    // The target field of the prefix reference path must be a reference field
                    if (!(prefixReferencePath.targetFieldInfo instanceof JReferenceFieldInfo)) {
                        final String targetFieldName = prefixPath.replaceAll("^.*\\.([^.]+)$", "$1");
                        throw new IllegalArgumentException(OnChangeScanner.this.getErrorPrefix(method) + "field `"
                          + targetFieldName + "' in " + prefixReferencePath.getTargetType() + " is not a reference field");
                    }

                    // Replace path with list of non-wildcard paths for every field in the target field type
                    for (JClass<?> jclass : jdb.getJClasses(
                      prefixReferencePath.getTargetFieldTypes().iterator().next().getRawType())) {
                        for (JField jfield : jclass.jfields.values()) {
                            expandedPathWasWildcard.add(expandedPathList.size());
                            expandedPathList.add(prefixPath + "." + jfield.name + "#" + jfield.storageId);
                        }
                    }
                    continue;
                }

                // Non-wildcard path
                expandedPathList.add(unexpandedPath);
            }

            // Get method parameter type (generic and raw), if any, and extract generic types from the FieldChange<?> parameter
            final TypeToken<?> genericParameterType;
            final Class<?> rawParameterType;
            switch (method.getParameterTypes().length) {
            case 1:
                rawParameterType = method.getParameterTypes()[0];
                genericParameterType = OnChangeScanner.this.getParameterTypeTokens(method).get(0);
                final Type firstParameterType = method.getGenericParameterTypes()[0];
                if (firstParameterType instanceof ParameterizedType) {
                    final ArrayList<Class<?>> genericTypeList = new ArrayList<>(3);
                    for (Type type : ((ParameterizedType)firstParameterType).getActualTypeArguments())
                        genericTypeList.add(TypeToken.of(type).getRawType());
                    this.genericTypes = genericTypeList.toArray(new Class<?>[genericTypeList.size()]);
                } else
                    this.genericTypes = new Class<?>[] { rawParameterType };
                break;
            case 0:
                rawParameterType = null;
                genericParameterType = null;
                this.genericTypes = null;
                break;
            default:
                throw new RuntimeException("internal error");
            }

            // Parse reference paths
            boolean anyFieldsFound = false;
            this.paths = new HashMap<>(expandedPathList.size());
            for (int i = 0; i < expandedPathList.size(); i++) {
                final String stringPath = expandedPathList.get(i);
                final boolean wildcard = expandedPathWasWildcard.contains(i);           // path was auto-generated from a wildcard

                // Parse reference path
                final ReferencePath path;
                try {
                    path = jdb.parseReferencePath(startType, stringPath, false);
                } catch (IllegalArgumentException e) {
                    throw new IllegalArgumentException(OnChangeScanner.this.getErrorPrefix(method) + e.getMessage(), e);
                }

                // Get the actual types and storage ID's of all model classes in the path that actually contain the target field
                final HashSet<Class<?>> targetTypes = new HashSet<>();
                final HashSet<Integer> storageIds = new HashSet<>();
                jdb.getJClasses(path.targetTypes.iterator().next()).stream()
                  .filter(jclass -> jclass.jfields.containsKey(path.targetFieldInfo.storageId))
                  .forEach(jclass -> {
                    targetTypes.add(jclass.getType());
                    storageIds.add(jclass.storageId);
                });

                // Validate the parameter type against the types of possible change events
                if (rawParameterType != null) {

                    // Get all possible (concrete) change types emitted by the target field
                    final ArrayList<TypeToken<?>> possibleChangeTypes = new ArrayList<TypeToken<?>>();
                    for (Class<?> targetType : targetTypes) {
                        try {
                            path.targetFieldInfo.addChangeParameterTypes(possibleChangeTypes, targetType);
                        } catch (UnsupportedOperationException e) {
                            if (wildcard)
                                continue;
                            throw new IllegalArgumentException(OnChangeScanner.this.getErrorPrefix(method) + "path `" + stringPath
                              + "' is invalid because change notifications are not supported for " + path.targetFieldInfo, e);
                        }
                        anyFieldsFound = true;
                    }

                    // Check whether method parameter type accepts as least one of them; it must do so consistently raw vs. generic
                    boolean anyChangeMatch = false;
                    for (TypeToken<?> possibleChangeType : possibleChangeTypes) {
                        final boolean matchesGeneric = genericParameterType.isSupertypeOf(possibleChangeType);
                        final boolean matchesRaw = rawParameterType.isAssignableFrom(possibleChangeType.getRawType());
                        assert !matchesGeneric || matchesRaw;
                        if (matchesGeneric != matchesRaw) {
                            throw new IllegalArgumentException(OnChangeScanner.this.getErrorPrefix(method)
                              + "parameter type " + genericParameterType + " will match change events of type "
                              + possibleChangeType + " from field `" + stringPath + "' at runtime due to type erasure,"
                              + " but its generic type is incompatible; parameter type should be compatible with "
                              + (possibleChangeTypes.size() != 1 ?
                                  "one or more of: " + possibleChangeTypes : possibleChangeTypes.get(0)));
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
                          + (possibleChangeTypes.size() != 1 ? "one of: " + possibleChangeTypes : possibleChangeTypes.get(0)));
                    }
                }

                // Match on storage ID's; this filters out obsolete types from old versions
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
        void registerChangeListener(Transaction tx) {
            for (Map.Entry<ReferencePath, HashSet<Integer>> entry : this.paths.entrySet()) {
                final ReferencePath path = entry.getKey();
                final HashSet<Integer> objectTypeStorageIds = entry.getValue();
                path.targetFieldInfo.registerChangeListener(tx, path.getReferenceFields(), objectTypeStorageIds, this);
            }
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == this)
                return true;
            if (!super.equals(obj))
                return false;
            final OnChangeScanner<?>.ChangeMethodInfo that = (OnChangeScanner<?>.ChangeMethodInfo)obj;
            return this.paths.equals(that.paths);
        }

        @Override
        public int hashCode() {
            return super.hashCode() ^ this.paths.hashCode();
        }

    // SimpleFieldChangeListener

        @Override
        @SuppressWarnings({ "unchecked", "rawtypes" })
        public <T> void onSimpleFieldChange(Transaction tx, ObjId id,
          SimpleField<T> field, int[] path, NavigableSet<ObjId> referrers, T oldValue, T newValue) {
            final JTransaction jtx = (JTransaction)tx.getUserObject();
            assert jtx != null && jtx.tx == tx;
            if (this.genericTypes == null) {
                this.invoke(jtx, referrers);
                return;
            }
            final Object joldValue = jtx.convertCoreValue(field, oldValue);
            final Object jnewValue = jtx.convertCoreValue(field, newValue);
            final JObject jobj = this.checkTypes(jtx, SimpleFieldChange.class, id, joldValue, jnewValue);
            if (jobj == null)
                return;
            this.invoke(jtx, referrers, new SimpleFieldChange(jobj, field.getStorageId(), field.getName(), joldValue, jnewValue));
        }

    // SetFieldChangeListener

        @Override
        @SuppressWarnings({ "unchecked", "rawtypes" })
        public <E> void onSetFieldAdd(Transaction tx, ObjId id,
          SetField<E> field, int[] path, NavigableSet<ObjId> referrers, E value) {
            final JTransaction jtx = (JTransaction)tx.getUserObject();
            assert jtx != null && jtx.tx == tx;
            if (this.genericTypes == null) {
                this.invoke(jtx, referrers);
                return;
            }
            final Object jvalue = jtx.convertCoreValue(field.getElementField(), value);
            final JObject jobj = this.checkTypes(jtx, SetFieldAdd.class, id, jvalue);
            if (jobj == null)
                return;
            this.invoke(jtx, referrers, new SetFieldAdd(jobj, field.getStorageId(), field.getName(), jvalue));
        }

        @Override
        @SuppressWarnings({ "unchecked", "rawtypes" })
        public <E> void onSetFieldRemove(Transaction tx, ObjId id,
          SetField<E> field, int[] path, NavigableSet<ObjId> referrers, E value) {
            final JTransaction jtx = (JTransaction)tx.getUserObject();
            assert jtx != null && jtx.tx == tx;
            if (this.genericTypes == null) {
                this.invoke(jtx, referrers);
                return;
            }
            final Object jvalue = jtx.convertCoreValue(field.getElementField(), value);
            final JObject jobj = this.checkTypes(jtx, SetFieldRemove.class, id, jvalue);
            if (jobj == null)
                return;
            this.invoke(jtx, referrers, new SetFieldRemove(jobj, field.getStorageId(), field.getName(), jvalue));
        }

        @Override
        public void onSetFieldClear(Transaction tx, ObjId id, SetField<?> field, int[] path, NavigableSet<ObjId> referrers) {
            final JTransaction jtx = (JTransaction)tx.getUserObject();
            assert jtx != null && jtx.tx == tx;
            if (this.genericTypes == null) {
                this.invoke(jtx, referrers);
                return;
            }
            final JObject jobj = this.checkTypes(jtx, SetFieldClear.class, id);
            if (jobj == null)
                return;
            this.invoke(jtx, referrers, new SetFieldClear<>(jobj, field.getStorageId(), field.getName()));
        }

    // ListFieldChangeListener

        @Override
        @SuppressWarnings({ "unchecked", "rawtypes" })
        public <E> void onListFieldAdd(Transaction tx, ObjId id,
          ListField<E> field, int[] path, NavigableSet<ObjId> referrers, int index, E value) {
            final JTransaction jtx = (JTransaction)tx.getUserObject();
            assert jtx != null && jtx.tx == tx;
            if (this.genericTypes == null) {
                this.invoke(jtx, referrers);
                return;
            }
            final Object jvalue = jtx.convertCoreValue(field.getElementField(), value);
            final JObject jobj = this.checkTypes(jtx, ListFieldAdd.class, id, jvalue);
            if (jobj == null)
                return;
            this.invoke(jtx, referrers, new ListFieldAdd(jobj, field.getStorageId(), field.getName(), index, jvalue));
        }

        @Override
        @SuppressWarnings({ "unchecked", "rawtypes" })
        public <E> void onListFieldRemove(Transaction tx, ObjId id,
          ListField<E> field, int[] path, NavigableSet<ObjId> referrers, int index, E value) {
            final JTransaction jtx = (JTransaction)tx.getUserObject();
            assert jtx != null && jtx.tx == tx;
            if (this.genericTypes == null) {
                this.invoke(jtx, referrers);
                return;
            }
            final Object jvalue = jtx.convertCoreValue(field.getElementField(), value);
            final JObject jobj = this.checkTypes(jtx, ListFieldRemove.class, id, jvalue);
            if (jobj == null)
                return;
            this.invoke(jtx, referrers, new ListFieldRemove(jobj, field.getStorageId(), field.getName(), index, jvalue));
        }

        @Override
        @SuppressWarnings({ "unchecked", "rawtypes" })
        public <E> void onListFieldReplace(Transaction tx, ObjId id,
          ListField<E> field, int[] path, NavigableSet<ObjId> referrers, int index, E oldValue, E newValue) {
            final JTransaction jtx = (JTransaction)tx.getUserObject();
            assert jtx != null && jtx.tx == tx;
            if (this.genericTypes == null) {
                this.invoke(jtx, referrers);
                return;
            }
            final Object joldValue = jtx.convertCoreValue(field.getElementField(), oldValue);
            final Object jnewValue = jtx.convertCoreValue(field.getElementField(), newValue);
            final JObject jobj = this.checkTypes(jtx, ListFieldReplace.class, id, joldValue, jnewValue);
            if (jobj == null)
                return;
            this.invoke(jtx, referrers,
              new ListFieldReplace(jobj, field.getStorageId(), field.getName(), index, joldValue, jnewValue));
        }

        @Override
        public void onListFieldClear(Transaction tx, ObjId id, ListField<?> field, int[] path, NavigableSet<ObjId> referrers) {
            final JTransaction jtx = (JTransaction)tx.getUserObject();
            assert jtx != null && jtx.tx == tx;
            if (this.genericTypes == null) {
                this.invoke(jtx, referrers);
                return;
            }
            final JObject jobj = this.checkTypes(jtx, ListFieldClear.class, id);
            if (jobj == null)
                return;
            this.invoke(jtx, referrers, new ListFieldClear<>(jobj, field.getStorageId(), field.getName()));
        }

    // MapFieldChangeListener

        @Override
        @SuppressWarnings({ "unchecked", "rawtypes" })
        public <K, V> void onMapFieldAdd(Transaction tx, ObjId id,
          MapField<K, V> field, int[] path, NavigableSet<ObjId> referrers, K key, V value) {
            final JTransaction jtx = (JTransaction)tx.getUserObject();
            assert jtx != null && jtx.tx == tx;
            if (this.genericTypes == null) {
                this.invoke(jtx, referrers);
                return;
            }
            final Object jkey = jtx.convertCoreValue(field.getKeyField(), key);
            final Object jvalue = jtx.convertCoreValue(field.getValueField(), value);
            final JObject jobj = this.checkTypes(jtx, MapFieldAdd.class, id, jkey, jvalue);
            if (jobj == null)
                return;
            this.invoke(jtx, referrers, new MapFieldAdd(jobj, field.getStorageId(), field.getName(), jkey, jvalue));
        }

        @Override
        @SuppressWarnings({ "unchecked", "rawtypes" })
        public <K, V> void onMapFieldRemove(Transaction tx, ObjId id,
          MapField<K, V> field, int[] path, NavigableSet<ObjId> referrers, K key, V value) {
            final JTransaction jtx = (JTransaction)tx.getUserObject();
            assert jtx != null && jtx.tx == tx;
            if (this.genericTypes == null) {
                this.invoke(jtx, referrers);
                return;
            }
            final Object jkey = jtx.convertCoreValue(field.getKeyField(), key);
            final Object jvalue = jtx.convertCoreValue(field.getValueField(), value);
            final JObject jobj = this.checkTypes(jtx, MapFieldRemove.class, id, jkey, jvalue);
            if (jobj == null)
                return;
            this.invoke(jtx, referrers, new MapFieldRemove(jobj, field.getStorageId(), field.getName(), jkey, jvalue));
        }

        @Override
        @SuppressWarnings({ "unchecked", "rawtypes" })
        public <K, V> void onMapFieldReplace(Transaction tx, ObjId id,
          MapField<K, V> field, int[] path, NavigableSet<ObjId> referrers, K key, V oldValue, V newValue) {
            final JTransaction jtx = (JTransaction)tx.getUserObject();
            assert jtx != null && jtx.tx == tx;
            if (this.genericTypes == null) {
                this.invoke(jtx, referrers);
                return;
            }
            final Object jkey = jtx.convertCoreValue(field.getKeyField(), key);
            final Object joldValue = jtx.convertCoreValue(field.getValueField(), oldValue);
            final Object jnewValue = jtx.convertCoreValue(field.getValueField(), newValue);
            final JObject jobj = this.checkTypes(jtx, MapFieldReplace.class, id, jkey, joldValue, jnewValue);
            if (jobj == null)
                return;
            this.invoke(jtx, referrers,
              new MapFieldReplace(jobj, field.getStorageId(), field.getName(), jkey, joldValue, jnewValue));
        }

        @Override
        public void onMapFieldClear(Transaction tx, ObjId id, MapField<?, ?> field, int[] path, NavigableSet<ObjId> referrers) {
            final JTransaction jtx = (JTransaction)tx.getUserObject();
            assert jtx != null && jtx.tx == tx;
            if (this.genericTypes == null) {
                this.invoke(jtx, referrers);
                return;
            }
            final JObject jobj = this.checkTypes(jtx, MapFieldClear.class, id);
            if (jobj == null)
                return;
            this.invoke(jtx, referrers, new MapFieldClear<>(jobj, field.getStorageId(), field.getName()));
        }

    // Internal methods

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

        // Used when @OnChange method takes zero parameters
        private void invoke(JTransaction jtx, NavigableSet<ObjId> referrers) {
            final Method method = this.getMethod();
            if ((method.getModifiers() & Modifier.STATIC) != 0)
                Util.invoke(method, null);
            else {
                for (ObjId id : referrers) {
                    final JObject target = jtx.get(id);             // type of 'id' should always be found

                    // Avoid invoking subclass's @OnChange method on superclass instance;
                    // this can happen when the field is in superclass but wildcard @OnChange is in the subclass
                    if (method.getDeclaringClass().isInstance(target))
                        Util.invoke(method, target);
                }
            }
        }

        // Used when @OnChange method takes one parameter
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

