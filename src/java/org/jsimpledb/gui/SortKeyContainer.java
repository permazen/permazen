
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.gui;

import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import com.google.common.reflect.TypeToken;
import com.vaadin.ui.DefaultFieldFactory;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.NavigableSet;
import java.util.SortedMap;

import org.dellroad.stuff.vaadin7.ProvidesProperty;
import org.dellroad.stuff.vaadin7.SelfKeyedContainer;
import org.dellroad.stuff.vaadin7.VaadinConfigurable;
import org.jsimpledb.JComplexField;
import org.jsimpledb.JField;
import org.jsimpledb.JObject;
import org.jsimpledb.JSimpleDB;
import org.jsimpledb.JSimpleField;
import org.jsimpledb.JTransaction;
import org.jsimpledb.core.ObjId;
import org.jsimpledb.util.NavigableSets;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;

/**
 * Container that contains all possible sort keys for a given {@link JClass}.
 */
@SuppressWarnings("serial")
@VaadinConfigurable(preConstruction = true)
class SortKeyContainer extends SelfKeyedContainer<SortKeyContainer.SortKey> {

    public static final String DESCRIPTION_PROPERTY = "description";

    @Autowired
    @Qualifier("jsimpledbGuiJSimpleDB")
    private JSimpleDB jdb;

    /**
     * Constructor.
     *
     * @param type type restriction, or null for no restriction
     */
    public SortKeyContainer(Class<?> type) {
        super(SortKey.class);

        // Add sort keys common to all objects
        final ArrayList<SortKey> sortKeys = new ArrayList<>();
        sortKeys.add(new ObjectIdSortKey());
        sortKeys.add(new VersionSortKey());

        // Identify fields common to all sub-types of `type'
        SortedMap<Integer, JField> commonFields = Util.getCommonJFields(
          this.jdb.getJClasses(TypeToken.of(type != null ? type : Object.class)));

        // Add sort keys for all indexed fields common to all sub-types
        if (commonFields != null) {
            for (JField jfield : commonFields.values()) {
                if (jfield instanceof JComplexField) {
                    for (JSimpleField subField : ((JComplexField)jfield).getSubFields()) {
                        if (subField.isIndexed())
                            sortKeys.add(new FieldSortKey(subField));
                    }
                } else if (jfield instanceof JSimpleField && ((JSimpleField)jfield).isIndexed())
                    sortKeys.add(new FieldSortKey((JSimpleField)jfield));
            }
        }

        // Load container
        this.load(sortKeys);
    }

// SortKey

    abstract class SortKey implements ObjectContainer.Query {

        private final String description;

        SortKey(String description) {
            this.description = description;
        }

        @ProvidesProperty(DESCRIPTION_PROPERTY)
        public String getDescription() {
            return this.description;
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == this)
                return true;
            if (obj == null || obj.getClass() != this.getClass())
                return false;
            return true;
        }

        @Override
        public int hashCode() {
            return this.getClass().hashCode();
        }

        @Override
        public String toString() {
            return "SortKey[" + this.description + "]";
        }
    }

    // Sorts by object ID
    class ObjectIdSortKey extends SortKey {

        ObjectIdSortKey() {
            super("Object ID");
        }

        @Override
        public <T> NavigableSet<T> query(Class<T> type) {
            return JTransaction.getCurrent().getAll(type);
        }
    }

    // Sorts by schema version, then object ID
    class VersionSortKey extends SortKey {

        VersionSortKey() {
            super("Version");
        }

        @Override
        public <T> Iterable<T> query(Class<T> type) {
            return Iterables.concat(
              Iterables.transform(JTransaction.getCurrent().queryVersion().values(), new TypeIntersector<T>(type)));
        }
    }

    // Sorts by indexed field, then object ID
    class FieldSortKey extends SortKey {

        private final int storageId;
        private final boolean isSubField;

        FieldSortKey(JSimpleField jfield) {
            super((jfield.getParentField() != null ?
               DefaultFieldFactory.createCaptionByPropertyId(jfield.getParentField().getName()) + "." : "")
              + DefaultFieldFactory.createCaptionByPropertyId(jfield.getName()));
            this.storageId = jfield.getStorageId();
            this.isSubField = jfield.getParentField() != null;
        }

        @Override
        public <T> Iterable<T> query(Class<T> type) {

            // Query index, intersect each value set with the given type, and concatenate the resulting sets
            Iterable<T> values = Iterables.concat(Iterables.transform(
              JTransaction.getCurrent().querySimpleField(this.storageId).values(), new TypeIntersector<T>(type)));

            // For sub-fields of complex fields, objects can appear multiple times so we have to filter out duplicates
            if (this.isSubField) {
                values = Iterables.filter(values, new Predicate<T>() {
                    private final HashSet<ObjId> seen = new HashSet<>();
                    @Override
                    public boolean apply(T obj) {
                        return this.seen.add(((JObject)obj).getObjId());
                    }
                });
            }

            // Done
            return values;
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == this)
                return true;
            if (!super.equals(obj))
                return false;
            final FieldSortKey that = (FieldSortKey)obj;
            return this.storageId == that.storageId;
        }

        @Override
        public int hashCode() {
            return super.hashCode() ^ this.storageId;
        }
    }

    // Creates intersection with the specified type
    private static class TypeIntersector<T> implements Function<NavigableSet<JObject>, NavigableSet<T>> {

        private final NavigableSet<T> typeSet;

        TypeIntersector(Class<T> type) {
            this.typeSet = type != null ? JTransaction.getCurrent().getAll(type) : null;
        }

        @Override
        @SuppressWarnings("unchecked")
        public NavigableSet<T> apply(NavigableSet<JObject> set) {
            return this.typeSet != null ? NavigableSets.intersection((NavigableSet<T>)set, this.typeSet) : (NavigableSet<T>)set;
        }
    }
}

