
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb;

import java.lang.reflect.Method;
import java.util.List;

import org.jsimpledb.schema.ListSchemaField;
import org.objectweb.asm.ClassWriter;

/**
 * Represents a list field in a {@link JClass}.
 */
public class JListField extends JCollectionField {

    JListField(JSimpleDB jdb, String name, int storageId, JSimpleField elementField, String description, Method getter) {
        super(jdb, name, storageId, elementField, description, getter);
    }

    @Override
    public List<?> getValue(JObject jobj) {
        if (jobj == null)
            throw new IllegalArgumentException("null jobj");
        return jobj.getTransaction().readListField(jobj, this.storageId, false);
    }

    @Override
    public <R> R visit(JFieldSwitch<R> target) {
        return target.caseJListField(this);
    }

    @Override
    ListSchemaField toSchemaItem(JSimpleDB jdb) {
        final ListSchemaField schemaField = new ListSchemaField();
        super.initialize(jdb, schemaField);
        return schemaField;
    }

    @Override
    void outputMethods(ClassGenerator<?> generator, ClassWriter cw) {
        this.outputReadMethod(generator, cw, ClassGenerator.READ_LIST_FIELD_METHOD);
    }

    @Override
    JListFieldInfo toJFieldInfo() {
        return new JListFieldInfo(this);
    }
}

