
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb;

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.List;
import java.util.NavigableMap;

import org.jsimpledb.core.MapField;
import org.jsimpledb.schema.MapSchemaField;
import org.objectweb.asm.ClassWriter;

/**
 * Represents a map field in a {@link JClass}.
 */
public class JMapField extends JComplexField {

    final JSimpleField keyField;
    final JSimpleField valueField;

    JMapField(JSimpleDB jdb, String name, int storageId,
      JSimpleField keyField, JSimpleField valueField, String description, Method getter) {
        super(jdb, name, storageId, description, getter);
        if (keyField == null)
            throw new IllegalArgumentException("null keyField");
        if (valueField == null)
            throw new IllegalArgumentException("null valueField");
        this.keyField = keyField;
        this.valueField = valueField;
    }

    /**
     * Get the key sub-field.
     *
     * @return this field's key sub-field
     */
    public JSimpleField getKeyField() {
        return this.keyField;
    }

    /**
     * Get the value sub-field.
     *
     * @return this field's value sub-field
     */
    public JSimpleField getValueField() {
        return this.valueField;
    }

    @Override
    public NavigableMap<?, ?> getValue(JObject jobj) {
        if (jobj == null)
            throw new IllegalArgumentException("null jobj");
        return jobj.getTransaction().readMapField(jobj, this.storageId, false);
    }

    @Override
    public <R> R visit(JFieldSwitch<R> target) {
        return target.caseJMapField(this);
    }

    @Override
    public List<JSimpleField> getSubFields() {
        return Arrays.asList(this.keyField, this.valueField);
    }

    @Override
    public JSimpleField getSubField(String name) {
        if (MapField.KEY_FIELD_NAME.equals(name))
            return this.keyField;
        if (MapField.VALUE_FIELD_NAME.equals(name))
            return this.valueField;
        throw new IllegalArgumentException("unknown sub-field `" + name
          + "' (did you mean `" + MapField.KEY_FIELD_NAME + "' or `" + MapField.VALUE_FIELD_NAME + "' instead?)");
    }

    @Override
    String getSubFieldName(JSimpleField subField) {
        if (subField == this.keyField)
            return MapField.KEY_FIELD_NAME;
        if (subField == this.valueField)
            return MapField.VALUE_FIELD_NAME;
        throw new IllegalArgumentException("unknown sub-field");
    }

    @Override
    MapSchemaField toSchemaItem(JSimpleDB jdb) {
        final MapSchemaField schemaField = new MapSchemaField();
        super.initialize(jdb, schemaField);
        schemaField.setKeyField(this.keyField.toSchemaItem(jdb));
        schemaField.setValueField(this.valueField.toSchemaItem(jdb));
        return schemaField;
    }

    @Override
    void outputMethods(ClassGenerator<?> generator, ClassWriter cw) {
        this.outputReadMethod(generator, cw, ClassGenerator.READ_MAP_FIELD_METHOD);
    }

    @Override
    JMapFieldInfo toJFieldInfo() {
        return new JMapFieldInfo(this);
    }
}

