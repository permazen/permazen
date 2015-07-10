
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb;

import com.google.common.base.Preconditions;

import java.lang.reflect.Method;
import java.util.NavigableSet;

import org.jsimpledb.schema.SetSchemaField;
import org.objectweb.asm.ClassWriter;

/**
 * Represents a set field in a {@link JClass}.
 */
public class JSetField extends JCollectionField {

    JSetField(JSimpleDB jdb, String name, int storageId, JSimpleField elementField, String description, Method getter) {
        super(jdb, name, storageId, elementField, description, getter);
    }

    @Override
    public NavigableSet<?> getValue(JObject jobj) {
        Preconditions.checkArgument(jobj != null, "null jobj");
        return jobj.getTransaction().readSetField(jobj.getObjId(), this.storageId, false);
    }

    @Override
    public <R> R visit(JFieldSwitch<R> target) {
        return target.caseJSetField(this);
    }

    @Override
    SetSchemaField toSchemaItem(JSimpleDB jdb) {
        final SetSchemaField schemaField = new SetSchemaField();
        super.initialize(jdb, schemaField);
        return schemaField;
    }

    @Override
    void outputMethods(ClassGenerator<?> generator, ClassWriter cw) {
        this.outputReadMethod(generator, cw, ClassGenerator.READ_SET_FIELD_METHOD);
    }

    @Override
    JSetFieldInfo toJFieldInfo() {
        return new JSetFieldInfo(this);
    }
}

