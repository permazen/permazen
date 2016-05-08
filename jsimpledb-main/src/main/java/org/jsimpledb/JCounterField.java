
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb;

import com.google.common.base.Preconditions;

import java.lang.reflect.Method;

import org.jsimpledb.schema.CounterSchemaField;
import org.objectweb.asm.ClassWriter;

/**
 * Represents a counter field in a {@link JClass}.
 *
 * @see org.jsimpledb.Counter
 */
public class JCounterField extends JField {

    JCounterField(JSimpleDB jdb, String name, int storageId, String description, Method getter) {
        super(jdb, name, storageId, description, getter);
    }

    @Override
    public Counter getValue(JObject jobj) {
        Preconditions.checkArgument(jobj != null, "null jobj");
        return jobj.getTransaction().readCounterField(jobj.getObjId(), this.storageId, false);
    }

    @Override
    public <R> R visit(JFieldSwitch<R> target) {
        return target.caseJCounterField(this);
    }

    @Override
    CounterSchemaField toSchemaItem(JSimpleDB jdb) {
        final CounterSchemaField schemaField = new CounterSchemaField();
        this.initialize(jdb, schemaField);
        return schemaField;
    }

    @Override
    JCounterFieldInfo toJFieldInfo() {
        return new JCounterFieldInfo(this);
    }

// Bytecode generation

    @Override
    void outputFields(ClassGenerator<?> generator, ClassWriter cw) {
        this.outputCachedValueField(generator, cw);
    }

    @Override
    void outputMethods(ClassGenerator<?> generator, ClassWriter cw) {
        this.outputCachedValueGetterMethod(generator, cw, ClassGenerator.READ_COUNTER_FIELD_METHOD);
    }
}

