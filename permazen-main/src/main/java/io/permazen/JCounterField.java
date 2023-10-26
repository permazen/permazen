
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen;

import com.google.common.base.Converter;
import com.google.common.base.Preconditions;
import com.google.common.reflect.TypeToken;

import io.permazen.core.ObjId;
import io.permazen.schema.CounterSchemaField;

import java.lang.reflect.Method;
import java.util.List;

import org.objectweb.asm.ClassWriter;

/**
 * Represents a counter field in a {@link JClass}.
 *
 * @see Counter
 */
public class JCounterField extends JField {

    final UpgradeConversionPolicy upgradeConversion;

    JCounterField(Permazen jdb, String name, int storageId,
      io.permazen.annotation.JField annotation, String description, Method getter) {
        super(jdb, name, storageId, annotation, description, getter);
        this.upgradeConversion = annotation.upgradeConversion();
    }

    @Override
    public io.permazen.annotation.JField getDeclaringAnnotation() {
        return (io.permazen.annotation.JField)super.getDeclaringAnnotation();
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
    CounterSchemaField toSchemaItem(Permazen jdb) {
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

// POJO import/export

    @Override
    void importPlain(ImportContext context, Object obj, ObjId id) {

        // Read POJO property
        final Object value;
        try {
            value = obj.getClass().getMethod(this.getter.getName()).invoke(obj);
        } catch (Exception e) {
            return;
        }

        // Auto-convert any Number
        final long count;
        if (value instanceof Number)
            count = ((Number)value).longValue();
        else
            return;

        // Set counter value
        context.getTransaction().getTransaction().writeCounterField(id, this.storageId, count, true);
    }

    @Override
    void exportPlain(ExportContext context, ObjId id, Object obj) {

        // Find setter method
        final Method objSetter;
        try {
            objSetter = Util.findJFieldSetterMethod(obj.getClass(), obj.getClass().getMethod(this.getter.getName()));
        } catch (Exception e) {
            return;
        }

        // Get counter value
        final long count = context.getTransaction().getTransaction().readCounterField(id, this.storageId, true);

        // Auto-convert any Number
        final Class<?> valueType = TypeToken.of(objSetter.getParameterTypes()[0]).wrap().getRawType();
        final Object value;
        if (valueType == Long.class)
            value = count;
        else if (valueType == Integer.class)
            value = (int)count;
        else if (valueType == Float.class)
            value = (float)count;
        else if (valueType == Double.class)
            value = (double)count;
        else if (valueType == Byte.class)
            value = (byte)count;
        else if (valueType == Short.class)
            value = (short)count;
        else
            return;

        // Set POJO value
        try {
            objSetter.invoke(obj, value);
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new RuntimeException("failed to invoke setter method " + objSetter, e);
        }
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
