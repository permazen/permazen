
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb;

import com.google.common.reflect.TypeToken;

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
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
        return true;                                    // we check parameter type in ChangeMethodInfo
    }

    @Override
    protected ChangeMethodInfo createMethodInfo(Method method, OnChange annotation) {
        return new ChangeMethodInfo(method, annotation);
    }

// ChangeMethodInfo

    class ChangeMethodInfo extends MethodInfo implements AllChangesListener {

        final boolean isStatic;
        final ReferencePath path;
        final Class<? extends FieldChange<T>> changeType;

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

            // Parse reference path
            try {
                this.path = new ReferencePath(OnChangeScanner.this.jclass.jdb, startType, annotation.value(), false);
            } catch (IllegalArgumentException e) {
                throw new IllegalArgumentException(OnChangeScanner.this.getErrorPrefix(method) + e.getMessage(), e);
            }

            // Validate method parameter type
            final ArrayList<TypeToken<?>> changeParameterTypes = new ArrayList<TypeToken<?>>();
            this.path.targetField.addChangeParameterTypes(changeParameterTypes, this.path.targetType);
            OnChangeScanner.this.checkSingleParameterType(method, changeParameterTypes);

            // Save actual parameter type
            this.changeType = (Class<? extends FieldChange<T>>)method.getParameterTypes()[0];
        }

        // Register a listener for this method
        void registerChangeListener(Transaction tx) {
            this.path.targetField.registerChangeListener(tx, this.path.getReferenceFields(), this);
        }

        private Object convert(int index, Object obj) {
            if (obj == null)
                return null;
            boolean isReference;
            if (index == -1) {
                assert this.path.targetField instanceof JSimpleField;
                isReference = this.path.targetField instanceof JReferenceField;
            } else {
                assert this.path.targetField instanceof JComplexField;
                isReference = ((JComplexField)this.path.targetField).getSubFields().get(index) instanceof JReferenceField;
            }
            return isReference ? this.getJObject((ObjId)obj) : obj;
        }

    // SimpleFieldChangeListener

        @Override
        @SuppressWarnings({ "unchecked", "rawtypes" })
        public <T> void onSimpleFieldChange(Transaction tx, ObjId id,
          int storageId, int[] path, NavigableSet<ObjId> referrers, T oldValue, T newValue) {
            if (this.changeType.isAssignableFrom(SimpleFieldChange.class)) {
                this.invoke(referrers, new SimpleFieldChange(this.getJObject(id),
                  this.convert(-1, oldValue), this.convert(-1, newValue)));
            }
        }

    // SetFieldChangeListener

        @Override
        @SuppressWarnings({ "unchecked", "rawtypes" })
        public <E> void onSetFieldAdd(Transaction tx, ObjId id, int storageId, int[] path, NavigableSet<ObjId> referrers, E value) {
            if (this.changeType.isAssignableFrom(SetFieldAdd.class))
                this.invoke(referrers, new SetFieldAdd(this.getJObject(id), this.convert(0, value)));
        }

        @Override
        @SuppressWarnings({ "unchecked", "rawtypes" })
        public <E> void onSetFieldRemove(Transaction tx, ObjId id, int storageId, int[] path,
          NavigableSet<ObjId> referrers, E value) {
            if (this.changeType.isAssignableFrom(SetFieldRemove.class))
                this.invoke(referrers, new SetFieldRemove(this.getJObject(id), this.convert(0, value)));
        }

        @Override
        public <E> void onSetFieldClear(Transaction tx, ObjId id, int storageId, int[] path, NavigableSet<ObjId> referrers) {
            if (this.changeType.isAssignableFrom(SetFieldClear.class))
                this.invoke(referrers, new SetFieldClear<JObject, E>(this.getJObject(id)));
        }

    // ListFieldChangeListener

        @Override
        @SuppressWarnings({ "unchecked", "rawtypes" })
        public <E> void onListFieldAdd(Transaction tx, ObjId id, int storageId, int[] path,
          NavigableSet<ObjId> referrers, int index, E value) {
            if (this.changeType.isAssignableFrom(ListFieldAdd.class))
                this.invoke(referrers, new ListFieldAdd(this.getJObject(id), index, this.convert(0, value)));
        }

        @Override
        @SuppressWarnings({ "unchecked", "rawtypes" })
        public <E> void onListFieldRemove(Transaction tx, ObjId id, int storageId, int[] path,
          NavigableSet<ObjId> referrers, int index, E value) {
            if (this.changeType.isAssignableFrom(ListFieldRemove.class))
                this.invoke(referrers, new ListFieldRemove(this.getJObject(id), index, this.convert(0, value)));
        }

        @Override
        @SuppressWarnings({ "unchecked", "rawtypes" })
        public <E> void onListFieldReplace(Transaction tx, ObjId id, int storageId, int[] path,
          NavigableSet<ObjId> referrers, int index, E oldValue, E newValue) {
            if (this.changeType.isAssignableFrom(ListFieldReplace.class)) {
                this.invoke(referrers, new ListFieldReplace(this.getJObject(id),
                  index, this.convert(0, oldValue), this.convert(0, newValue)));
            }
        }

        @Override
        public <E> void onListFieldClear(Transaction tx, ObjId id, int storageId, int[] path, NavigableSet<ObjId> referrers) {
            if (this.changeType.isAssignableFrom(ListFieldClear.class))
                this.invoke(referrers, new ListFieldClear<JObject, E>(this.getJObject(id)));
        }

    // MapFieldChangeListener

        @Override
        @SuppressWarnings({ "unchecked", "rawtypes" })
        public <K, V> void onMapFieldAdd(Transaction tx, ObjId id, int storageId, int[] path, NavigableSet<ObjId> referrers,
          K key, V value) {
            if (this.changeType.isAssignableFrom(MapFieldAdd.class))
                this.invoke(referrers, new MapFieldAdd(this.getJObject(id), this.convert(0, key), this.convert(1, value)));
        }

        @Override
        @SuppressWarnings({ "unchecked", "rawtypes" })
        public <K, V> void onMapFieldRemove(Transaction tx, ObjId id, int storageId, int[] path, NavigableSet<ObjId> referrers,
          K key, V value) {
            if (this.changeType.isAssignableFrom(MapFieldRemove.class))
                this.invoke(referrers, new MapFieldRemove(this.getJObject(id), this.convert(0, key), this.convert(1, value)));
        }

        @Override
        @SuppressWarnings({ "unchecked", "rawtypes" })
        public <K, V> void onMapFieldReplace(Transaction tx, ObjId id, int storageId, int[] path, NavigableSet<ObjId> referrers,
          K key, V oldValue, V newValue) {
            if (this.changeType.isAssignableFrom(MapFieldReplace.class)) {
                this.invoke(referrers, new MapFieldReplace(this.getJObject(id),
                  this.convert(0, key), this.convert(1, oldValue), this.convert(1, newValue)));
            }
        }

        @Override
        public <K, V> void onMapFieldClear(Transaction tx, ObjId id, int storageId, int[] path, NavigableSet<ObjId> referrers) {
            if (this.changeType.isAssignableFrom(MapFieldClear.class))
                this.invoke(referrers, new MapFieldClear<JObject, K, V>(this.getJObject(id)));
        }

    // Internal methods

        private JObject getJObject(ObjId id) {
            return JTransaction.getCurrent().jdb.getJObject(id);
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
            Util.invoke(this.getMethod(), target, change);
        }
    }
}

