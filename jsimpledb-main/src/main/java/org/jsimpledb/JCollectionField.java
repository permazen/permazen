
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb;

import com.google.common.base.Preconditions;

import java.lang.reflect.Method;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import org.jsimpledb.core.CollectionField;
import org.jsimpledb.schema.CollectionSchemaField;

/**
 * Represents a collection field in a {@link JClass}.
 */
public abstract class JCollectionField extends JComplexField {

    final JSimpleField elementField;

    JCollectionField(JSimpleDB jdb, String name, int storageId, JSimpleField elementField, String description, Method getter) {
        super(jdb, name, storageId, description, getter);
        Preconditions.checkArgument(elementField != null, "null elementField");
        this.elementField = elementField;
    }

    /**
     * Get the element sub-field.
     *
     * @return this instance's element sub-field
     */
    public JSimpleField getElementField() {
        return this.elementField;
    }

    @Override
    public abstract Collection<?> getValue(JObject jobj);

    @Override
    public List<JSimpleField> getSubFields() {
        return Collections.<JSimpleField>singletonList(this.elementField);
    }

    @Override
    public JSimpleField getSubField(String name) {
        if (CollectionField.ELEMENT_FIELD_NAME.equals(name))
            return this.elementField;
        throw new IllegalArgumentException("unknown sub-field `"
          + name + "' (did you mean `" + CollectionField.ELEMENT_FIELD_NAME + "' instead?)");
    }

    @Override
    String getSubFieldName(JSimpleField subField) {
        if (subField == this.elementField)
            return CollectionField.ELEMENT_FIELD_NAME;
        throw new IllegalArgumentException("unknown sub-field");
    }

    @Override
    abstract CollectionSchemaField toSchemaItem(JSimpleDB jdb);

    void initialize(JSimpleDB jdb, CollectionSchemaField schemaField) {
        super.initialize(jdb, schemaField);
        schemaField.setElementField(this.elementField.toSchemaItem(jdb));
    }
}

