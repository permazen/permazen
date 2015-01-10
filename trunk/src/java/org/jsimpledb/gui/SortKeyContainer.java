
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.gui;

import com.google.common.reflect.TypeToken;
import com.vaadin.ui.DefaultFieldFactory;

import java.util.ArrayList;
import java.util.SortedMap;

import org.dellroad.stuff.vaadin7.ProvidesProperty;
import org.dellroad.stuff.vaadin7.SelfKeyedContainer;
import org.jsimpledb.JClass;
import org.jsimpledb.JComplexField;
import org.jsimpledb.JField;
import org.jsimpledb.JObject;
import org.jsimpledb.JSimpleDB;
import org.jsimpledb.JSimpleField;
import org.jsimpledb.JTransaction;

/**
 * Container that contains all possible sort keys for a given {@link JClass}.
 */
@SuppressWarnings("serial")
class SortKeyContainer extends SelfKeyedContainer<SortKeyContainer.SortKey> {

    public static final String DESCRIPTION_PROPERTY = "description";

    private final JSimpleDB jdb;
    private final JClass<?> jclass;
    private final Class<?> type;

    public SortKeyContainer(JSimpleDB jdb, JClass<?> jclass) {
        this(jdb, jclass, jclass.getTypeToken().getRawType());
    }

    public SortKeyContainer(JSimpleDB jdb, Class<?> type) {
        this(jdb, null, type);
    }

    private SortKeyContainer(JSimpleDB jdb, JClass<?> jclass, Class<?> type) {
        super(SortKey.class);
        this.jdb = jdb;
        this.jclass = jclass;
        this.type = type;

        // Add sort keys common to all objects
        final ArrayList<SortKey> sortKeys = new ArrayList<>();
        sortKeys.add(new ObjectIdSortKey());
        sortKeys.add(new VersionSortKey());

        // Identify fields common to all sub-types of `type'
        SortedMap<Integer, JField> commonFields = Util.getCommonJFields(
          this.jdb.getJClasses(TypeToken.of(this.type != null ? this.type : Object.class)));

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

    private String getAllExpression() {
        final StringBuilder buf = new StringBuilder();
        buf.append("all(");
        if (this.jclass != null)
            buf.append(this.jclass.getName());
        else if (this.type != null)
            buf.append(this.type.getName() + ".class");
        buf.append(")");
        return buf.toString();
    }

// SortKey

    abstract class SortKey {

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

        public abstract String getExpression(JObject startingPoint, boolean reverse);
    }

    // Sorts by object ID
    class ObjectIdSortKey extends SortKey {

        ObjectIdSortKey() {
            super("Object ID");
        }

        @Override
        public String getExpression(JObject startingPoint, boolean reverse) {       // TODO: starting point and sort order
            return SortKeyContainer.this.getAllExpression();
        }
    }

    // Sorts by schema version, then object ID
    class VersionSortKey extends SortKey {

        VersionSortKey() {
            super("Version");
        }

        @Override
        public String getExpression(JObject startingPoint, boolean reverse) {       // TODO: starting point and sort order
            String versions = "queryVersion().values()";
            final String typeAll = SortKeyContainer.this.getAllExpression();
            if (!typeAll.equals("all()"))
                versions = "transform(" + versions + ", $version," + " $version & " + typeAll + ")";
            return "concat(" + versions + ")";
        }
    }

    // Sorts by indexed field, then object ID
    class FieldSortKey extends SortKey {

        private final int storageId;
        private final String fieldName;
        private final boolean isSubField;

        FieldSortKey(JSimpleField jfield) {
            super((jfield.getParentField() != null ?
               DefaultFieldFactory.createCaptionByPropertyId(jfield.getParentField().getName()) + "." : "")
              + DefaultFieldFactory.createCaptionByPropertyId(jfield.getName()));
            this.storageId = jfield.getStorageId();
            this.isSubField = jfield.getParentField() != null;
            this.fieldName = (this.isSubField ? jfield.getParentField().getName() + "." : "") + jfield.getName();
        }

        @Override
        public String getExpression(JObject startingPoint, boolean reverse) {       // TODO: starting point and sort order
            String values = (SortKeyContainer.this.jclass != null ?
              "query(" + SortKeyContainer.this.jclass.getName() + "." + this.fieldName + ")" :
              JTransaction.class.getName() + ".getCurrent().queryIndex(" + this.storageId + ")") + ".values()";
            final String typeAll = SortKeyContainer.this.getAllExpression();
            if (!typeAll.equals("all()"))
                values = "transform(" + values + ", $value," + " $value & " + typeAll + ")";
            return "concat(" + values + ")";
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == this)
                return true;
            if (!super.equals(obj))
                return false;
            final FieldSortKey that = (FieldSortKey)obj;
            return this.storageId == that.storageId && this.isSubField == that.isSubField;
        }

        @Override
        public int hashCode() {
            return super.hashCode() ^ this.storageId ^ (this.isSubField ? 1 : 0);
        }
    }
}

