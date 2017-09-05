
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen;

import com.google.common.base.Converter;
import com.google.common.base.Preconditions;
import com.google.common.reflect.TypeToken;

import java.lang.reflect.Method;
import java.util.List;

import io.permazen.schema.CounterSchemaField;
import org.objectweb.asm.ClassWriter;

/**
 * Represents a counter field in a {@link JClass}.
 *
 * @see io.permazen.Counter
 */
public class JCounterField extends JField {

    final UpgradeConversionPolicy upgradeConversion;

    JCounterField(JSimpleDB jdb, String name, int storageId,
      io.permazen.annotation.JField annotation, String description, Method getter) {
        super(jdb, name, storageId, description, getter);
        this.upgradeConversion = annotation.upgradeConversion();
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
    public TypeToken<Counter> getTypeToken() {
        return TypeToken.of(Counter.class);
    }

    @Override
    public Converter<?, ?> getConverter(JTransaction jtx) {
        return null;
    }

    @Override
    boolean supportsChangeNotifications() {
        return false;
    }

    @Override
    <T> void addChangeParameterTypes(List<TypeToken<?>> types, Class<T> targetType) {
        throw new UnsupportedOperationException("counter fields do not support change notifications");
    }

// Bytecode generation

    @Override
    void outputFields(ClassGenerator<?> generator, ClassWriter cw) {
        this.outputCachedValueField(generator, cw);
    }

    @Override
    void outputMethods(ClassGenerator<?> generator, ClassWriter cw) {
        this.outputCachedNonSimpleValueGetterMethod(generator, cw, ClassGenerator.JTRANSACTION_READ_COUNTER_FIELD_METHOD);
    }
}

