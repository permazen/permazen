
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen;

import com.google.common.base.Converter;
import com.google.common.base.Preconditions;
import com.google.common.reflect.TypeToken;

import io.permazen.core.CounterField;
import io.permazen.core.ObjId;
import io.permazen.schema.CounterSchemaField;

import java.lang.reflect.Method;
import java.util.List;
import java.util.Objects;

import org.objectweb.asm.ClassWriter;

/**
 * Represents a counter field in a {@link PermazenClass}.
 *
 * @see Counter
 */
public class PermazenCounterField extends PermazenField {

    final UpgradeConversionPolicy upgradeConversion;

// Constructor

    PermazenCounterField(String name, int storageId,
      io.permazen.annotation.PermazenField annotation, String description, Method getter) {
        super(name, storageId, annotation, description, getter);
        this.upgradeConversion = annotation.upgradeConversion();
    }

// Public Methods

    @Override
    public io.permazen.annotation.PermazenField getDeclaringAnnotation() {
        return (io.permazen.annotation.PermazenField)super.getDeclaringAnnotation();
    }

    @Override
    public Counter getValue(PermazenObject pobj) {
        Preconditions.checkArgument(pobj != null, "null pobj");
        return pobj.getPermazenTransaction().readCounterField(pobj.getObjId(), this.name, false);
    }

    @Override
    public <R> R visit(PermazenFieldSwitch<R> target) {
        Preconditions.checkArgument(target != null, "null target");
        return target.casePermazenCounterField(this);
    }

    @Override
    public TypeToken<Counter> getTypeToken() {
        return TypeToken.of(Counter.class);
    }

    @Override
    public Converter<?, ?> getConverter(PermazenTransaction ptx) {
        return null;
    }

    @Override
    public CounterField getSchemaItem() {
        return (CounterField)super.getSchemaItem();
    }

// Package Methods

    @Override
    boolean isSameAs(PermazenField that0) {
        if (!super.isSameAs(that0))
            return false;
        final PermazenCounterField that = (PermazenCounterField)that0;
        if (!Objects.equals(this.upgradeConversion, that.upgradeConversion))
            return false;
        return true;
    }

    @Override
    CounterSchemaField createSchemaItem() {
        return new CounterSchemaField();
    }

    @Override
    <T> void addChangeParameterTypes(List<TypeToken<?>> types, Class<T> targetType) {
        throw new UnsupportedOperationException("counter fields do not support change notifications");
    }

    @Override
    boolean supportsChangeNotifications() {
        return false;
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
        context.getPermazenTransaction().getTransaction().writeCounterField(id, this.name, count, true);
    }

    @Override
    void exportPlain(ExportContext context, ObjId id, Object obj) {

        // Find setter method
        final Method objSetter;
        try {
            objSetter = Util.findPermazenFieldSetterMethod(obj.getClass(), obj.getClass().getMethod(this.getter.getName()));
        } catch (Exception e) {
            return;
        }

        // Get counter value
        final long count = context.getPermazenTransaction().getTransaction().readCounterField(id, this.name, true);

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
        this.outputCachedNonSimpleValueGetterMethod(generator, cw, ClassGenerator.PERMAZEN_TRANSACTION_READ_COUNTER_FIELD_METHOD);
    }
}
