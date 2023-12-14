
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.vaadin;

import com.vaadin.ui.DefaultFieldFactory;

import io.permazen.JClass;
import io.permazen.JComplexField;
import io.permazen.JField;
import io.permazen.JObject;
import io.permazen.JSimpleField;
import io.permazen.Permazen;
import io.permazen.parse.ParseSession;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.SortedMap;

import org.dellroad.stuff.string.StringEncoder;
import org.dellroad.stuff.vaadin7.ProvidesProperty;
import org.dellroad.stuff.vaadin7.SelfKeyedContainer;

/**
 * Container that contains all possible sort keys for a given {@link JClass}.
 */
@SuppressWarnings("serial")
class SortKeyContainer extends SelfKeyedContainer<SortKeyContainer.SortKey> {

    public static final String DESCRIPTION_PROPERTY = "description";

    private final Permazen jdb;
    private final JClass<?> jclass;
    private final Class<?> type;

    SortKeyContainer(Permazen jdb, JClass<?> jclass) {
        this(jdb, jclass, jclass.getType());
    }

    SortKeyContainer(Permazen jdb, Class<?> type) {
        this(jdb, null, type);
    }

    private SortKeyContainer(Permazen jdb, JClass<?> jclass, Class<?> type) {
        super(SortKey.class);
        this.jdb = jdb;
        this.jclass = jclass;
        this.type = type;

        // Add sort keys common to all objects
        final ArrayList<SortKey> sortKeys = new ArrayList<>();
        sortKeys.add(new ObjectIdSortKey());
        sortKeys.add(new VersionSortKey());

        // Identify fields common to all sub-types of `type'
        SortedMap<Integer, JField> commonFields = Util.getCommonJFields(this.jdb.getJClasses(this.type));

        // Add sort keys for all indexed fields common to all sub-types
        if (commonFields != null) {
            for (JField jfield : commonFields.values()) {
                if (jfield instanceof JComplexField) {
                    ((JComplexField)jfield).getSubFields().stream()
                      .filter(subField -> subField.isIndexed())
                      .map(FieldSortKey::new)
                      .iterator()
                      .forEachRemaining(sortKeys::add);
                } else if (jfield instanceof JSimpleField && ((JSimpleField)jfield).isIndexed())
                    sortKeys.add(new FieldSortKey((JSimpleField)jfield));
            }
        }

        // Sort indexed field sort keys
        Collections.sort(sortKeys.subList(2, sortKeys.size()), Comparator.comparing(SortKey::getDescription));

        // Load container
        this.load(sortKeys);
    }

    private String getTypeExpression(ParseSession session, boolean defaultJObject) {
        return this.jclass != null ? this.jclass.getName() :
          this.type != null ? session.relativizeClassName(this.type) + ".class" :
          defaultJObject ? session.relativizeClassName(Object.class) + ".class" : "";
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

        public abstract String getExpression(ParseSession session, JObject startingPoint, boolean reverse);
    }

    // Sorts by object ID
    class ObjectIdSortKey extends SortKey {

        ObjectIdSortKey() {
            super("Object ID");
        }

        @Override
        public String getExpression(ParseSession session, JObject startingPoint, boolean reverse) {       // TODO: starting point
            String expr = "all(" + SortKeyContainer.this.getTypeExpression(session, false) + ")";
            if (reverse)
                expr += ".descendingSet()";
            return expr;
        }
    }

    // Sorts by schema version, then object ID
    class VersionSortKey extends SortKey {

        VersionSortKey() {
            super("Schema Version");
        }

        @Override
        public String getExpression(ParseSession session, JObject startingPoint, boolean reverse) {       // TODO: starting point
            String expr = "queryVersion(" + SortKeyContainer.this.getTypeExpression(session, false) + ")";
            if (reverse)
                expr += ".descendingMap()";
            expr += ".values()";
            expr = "concat(\n  " + expr + ")";
            return expr;
        }
    }

    // Sorts by indexed field, then object ID
    class FieldSortKey extends SortKey {

        private final int storageId;
        private final String fieldName;
        private final Class<?> encoding;
        private final boolean isSubField;

        FieldSortKey(JSimpleField jfield) {
            super((jfield.getParentField() != null ?
               DefaultFieldFactory.createCaptionByPropertyId(jfield.getParentField().getName()) + "." : "")
              + DefaultFieldFactory.createCaptionByPropertyId(jfield.getName()));
            this.storageId = jfield.getStorageId();
            this.isSubField = jfield.getParentField() != null;
            this.fieldName = (this.isSubField ? jfield.getParentField().getName() + "." : "") + jfield.getName();
            this.encoding = jfield.getTypeToken().wrap().getRawType();
        }

        @Override
        public String getExpression(ParseSession session, JObject startingPoint, boolean reverse) {       // TODO: starting point
            String expr = "queryIndex(" + SortKeyContainer.this.getTypeExpression(session, true) + ", "
              + StringEncoder.enquote(this.fieldName) + ", " + session.relativizeClassName(this.encoding) + ".class).asMap()";
            if (reverse)
                expr += ".descendingMap()";
            expr += ".values()";
            expr = "concat(\n  " + expr + ")";
            return expr;
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
