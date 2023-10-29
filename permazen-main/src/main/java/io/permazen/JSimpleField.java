
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen;

import com.google.common.base.Converter;
import com.google.common.base.Preconditions;
import com.google.common.reflect.TypeParameter;
import com.google.common.reflect.TypeToken;

import io.permazen.change.SimpleFieldChange;
import io.permazen.core.EnumValue;
import io.permazen.core.ObjId;
import io.permazen.encoding.Encoding;
import io.permazen.encoding.EncodingId;
import io.permazen.schema.SimpleSchemaField;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;

import org.dellroad.stuff.java.Primitive;
import org.objectweb.asm.ClassWriter;
import org.objectweb.asm.Label;
import org.objectweb.asm.MethodVisitor;
import org.objectweb.asm.Opcodes;
import org.objectweb.asm.Type;

/**
 * Represents a simple field in a {@link JClass} or a simple sub-field of a complex field in a {@link JClass}.
 */
public class JSimpleField extends JField {

    final TypeToken<?> typeToken;
    final Encoding<?> encoding;
    final boolean indexed;
    final boolean unique;
    final ArrayList<Object> uniqueExcludes;         // note: these are core API values, sorted by this.encoding
    final UpgradeConversionPolicy upgradeConversion;
    final Method setter;

    @SuppressWarnings("unchecked")
    JSimpleField(Permazen jdb, String name, int storageId, TypeToken<?> typeToken, Encoding<?> encoding, boolean indexed,
      io.permazen.annotation.JField annotation, String description, Method getter, Method setter) {
        super(jdb, name, storageId, annotation, description, getter);
        Preconditions.checkArgument(typeToken != null, "null typeToken");
        Preconditions.checkArgument(encoding != null, "null encoding");
        this.typeToken = typeToken;
        this.encoding = encoding;
        this.indexed = indexed;
        this.unique = annotation.unique();
        this.setter = setter;
        this.upgradeConversion = annotation.upgradeConversion();

        // Parse uniqueExcludes
        final int numExcludes = annotation.uniqueExclude().length;
        if (numExcludes > 0) {
            assert this.unique;
            this.uniqueExcludes = new ArrayList<>(numExcludes);
            for (String string : annotation.uniqueExclude()) {
                if (string.equals(io.permazen.annotation.JField.NULL))
                    this.uniqueExcludes.add(null);
                else {
                    try {
                        this.uniqueExcludes.add(this.encoding.fromString(string));
                    } catch (IllegalArgumentException e) {
                        throw new IllegalArgumentException(
                          String.format("invalid uniqueExclude() value \"%s\": %s", string, e.getMessage()), e);
                    }
                }
            }
            Collections.sort(this.uniqueExcludes, (Comparator<Object>)this.encoding);
        } else
            this.uniqueExcludes = null;
    }

    @Override
    public io.permazen.annotation.JField getDeclaringAnnotation() {
        return (io.permazen.annotation.JField)super.getDeclaringAnnotation();
    }

    /**
     * Get the {@link JComplexField} of which this instance is a sub-field, if any.
     *
     * @return parent {@link JComplexField}, or null if this instance is not a sub-field
     */
    public JComplexField getParentField() {
        return this.parent instanceof JComplexField ? (JComplexField)this.parent : null;
    }

    /**
     * Get the type of this field.
     *
     * @return this field's Java type
     */
    public TypeToken<?> getTypeToken() {
        return this.typeToken;
    }

    /**
     * Get the {@link Encoding} used by the core API to encode this field's values.
     *
     * <p>
     * Note that for {@link Enum} and reference fields, the core API uses a different type than the Java model
     * classes ({@link EnumValue} and {@link ObjId}, respectively).
     * Values can always be properly converted using the {@link Converter} returned by {@link #getConverter getConverter()}.
     *
     * @return this field's core-layer type definition
     */
    public Encoding<?> getEncoding() {
        return this.encoding;
    }

    /**
     * Get whether this field is indexed.
     *
     * @return whether this field is indexed
     */
    public boolean isIndexed() {
        return this.indexed;
    }

    /**
     * Get the setter method associated with this field.
     *
     * @return field property setter method, or null if this field is a sub-field of a complex field
     */
    public Method getSetter() {
        return this.setter;
    }

    @Override
    public Object getValue(JObject jobj) {
        Preconditions.checkArgument(jobj != null, "null jobj");
        Preconditions.checkArgument(!(this.parent instanceof JComplexField), "field is a complex sub-field");
        return jobj.getTransaction().readSimpleField(jobj.getObjId(), this.storageId, false);
    }

    @Override
    public <R> R visit(JFieldSwitch<R> target) {
        return target.caseJSimpleField(this);
    }

    @Override
    public Converter<?, ?> getConverter(JTransaction jtx) {
        return null;
    }

    /**
     * Set the Java value of this field in the given object.
     * Does not alter the schema version of the object.
     *
     * @param jobj object containing this field
     * @param value new value
     * @throws io.permazen.core.DeletedObjectException if {@code jobj} does not exist in its associated {@link JTransaction}
     * @throws io.permazen.core.StaleTransactionException if the {@link JTransaction} associated with {@code jobj}
     *  is no longer usable
     * @throws IllegalArgumentException if this field is a sub-field of a complex field
     * @throws IllegalArgumentException if {@code value} is not an appropriate value for this field
     * @throws IllegalArgumentException if {@code jobj} is null
     */
    public void setValue(JObject jobj, Object value) {
        Preconditions.checkArgument(jobj != null, "null jobj");
        Preconditions.checkArgument(!(this.parent instanceof JComplexField), "field is a complex sub-field");
        jobj.getTransaction().writeSimpleField(jobj, this.storageId, value, false);
    }

    @Override
    boolean isSameAs(JField that0) {
        if (!super.isSameAs(that0))
            return false;
        final JSimpleField that = (JSimpleField)that0;
        if (!this.typeToken.equals(that.typeToken))
            return false;
        if (!this.encoding.equals(that.encoding))
            return false;
        if (this.indexed != that.indexed)
            return false;
        if (this.unique != that.unique)
            return false;
        if (!(Objects.equals(this.uniqueExcludes, that.uniqueExcludes)))
            return false;
        if (!this.upgradeConversion.equals(that.upgradeConversion))
            return false;
        return true;
    }

    @Override
    SimpleSchemaField toSchemaItem(Permazen jdb) {
        final SimpleSchemaField schemaField = new SimpleSchemaField();
        this.initialize(jdb, schemaField);
        return schemaField;
    }

    @Override
    SimpleFieldIndexInfo toIndexInfo() {
        if (!this.indexed)
            return null;
        final JComplexField parentField = this.getParentField();
        return parentField != null ? parentField.toIndexInfo(this) : new RegularSimpleFieldIndexInfo(this);
    }

    @Override
    void calculateRequiresDefaultValidation() {
        super.calculateRequiresDefaultValidation();
        this.requiresDefaultValidation |= this.unique;
    }

    void initialize(Permazen jdb, SimpleSchemaField schemaField) {
        super.initialize(jdb, schemaField);
        this.initializeSimpleSchemaFieldEncoding(schemaField);
        schemaField.setIndexed(this.indexed);
    }

    void initializeSimpleSchemaFieldEncoding(SimpleSchemaField schemaField) {
        final EncodingId encodingId = this.encoding.getEncodingId();
        Preconditions.checkArgument(encodingId != null, "internal error");
        schemaField.setEncodingId(encodingId);
    }

    @Override
    boolean supportsChangeNotifications() {
        return true;
    }

    @Override
    <T> void addChangeParameterTypes(List<TypeToken<?>> types, Class<T> targetType) {
        this.addChangeParameterTypes(types, targetType, this.typeToken);
    }

    // This method exists solely to bind the generic type parameters
    @SuppressWarnings("serial")
    private <T, V> void addChangeParameterTypes(List<TypeToken<?>> types, Class<T> targetType, TypeToken<V> encoding) {
        types.add(new TypeToken<SimpleFieldChange<T, V>>() { }
          .where(new TypeParameter<T>() { }, targetType)
          .where(new TypeParameter<V>() { }, encoding.wrap()));
    }

    // Get the Enum class that this type represents, or null if not applicable; see note in SimpleFieldIndexInfo
    Class<? extends Enum<?>> getEnumType() {
        return null;
    }

// POJO import/export

    @Override
    @SuppressWarnings("unchecked")
    void importPlain(ImportContext context, Object obj, ObjId id) {

        // Get POJO value
        final Object value;
        try {
            value = obj.getClass().getMethod(this.getter.getName()).invoke(obj);
        } catch (Exception e) {
            return;
        }
        final Object coreValue = this.importCoreValue(context, value);

        // Set field core API value
        context.getTransaction().getTransaction().writeSimpleField(id, this.storageId, coreValue, true);
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

        // Get field value
        final Object value = this.exportCoreValue(context,
          context.getTransaction().getTransaction().readSimpleField(id, this.storageId, true));

        // Set POJO value
        try {
            objSetter.invoke(obj, value);
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new RuntimeException("failed to invoke setter method " + objSetter, e);
        }
    }

    Object importCoreValue(ImportContext context, Object value) {
        return this.encoding.validate(value);
    }

    Object exportCoreValue(ExportContext context, Object value) {
        return value;
    }

// Bytecode generation

    @Override
    void outputFields(ClassGenerator<?> generator, ClassWriter cw) {

        // Output cached value field
        this.outputCachedValueField(generator, cw);
    }

    @Override
    void outputMethods(ClassGenerator<?> generator, ClassWriter cw) {

        // Get encoding
        final String className = generator.getClassName();
        final Class<?> propertyType = this.typeToken.getRawType();
        final boolean wide = propertyType.isPrimitive() && (Primitive.get(propertyType).getSize() == 8);

        // Start getter
        MethodVisitor mv = cw.visitMethod(
          this.getter.getModifiers() & (Opcodes.ACC_PUBLIC | Opcodes.ACC_PROTECTED),
          this.getter.getName(), Type.getMethodDescriptor(this.getter), null, generator.getExceptionNames(this.getter));

        // Return the cached value, if any
        this.emitGetCachedFlag(generator, mv);
        final Label notCached = new Label();
        mv.visitJumpInsn(Opcodes.IFEQ, notCached);
        mv.visitVarInsn(Opcodes.ALOAD, 0);
        mv.visitFieldInsn(Opcodes.GETFIELD, className, this.getCachedValueFieldName(), Type.getDescriptor(propertyType));
        mv.visitInsn(Type.getType(propertyType).getOpcode(Opcodes.IRETURN));
        mv.visitLabel(notCached);
        mv.visitFrame(Opcodes.F_SAME, 0, new Object[0], 0, new Object[0]);

        // Retrieve (and unwrap if necessary) the value
        mv.visitVarInsn(Opcodes.ALOAD, 0);
        this.outputReadCoreValueBytecode(generator, mv);
        mv.visitTypeInsn(Opcodes.CHECKCAST, Type.getInternalName(this.typeToken.wrap().getRawType()));
        if (propertyType.isPrimitive())
            generator.unwrap(mv, Primitive.get(propertyType));

        // Cache the retrieved value
        mv.visitInsn(wide ? Opcodes.DUP2_X1 : Opcodes.DUP_X1);
        mv.visitFieldInsn(Opcodes.PUTFIELD, className, this.getCachedValueFieldName(), Type.getDescriptor(propertyType));

        // Set the flag indicating cached value is valid
        this.emitSetCachedFlag(generator, mv, true);

        // Done with getter
        mv.visitInsn(Type.getType(propertyType).getOpcode(Opcodes.IRETURN));
        mv.visitMaxs(0, 0);
        mv.visitEnd();

        // Start setter
        mv = cw.visitMethod(
          this.setter.getModifiers() & (Opcodes.ACC_PUBLIC | Opcodes.ACC_PROTECTED),
          this.setter.getName(), Type.getMethodDescriptor(this.setter), null, generator.getExceptionNames(this.setter));

        // Invalidate cached value, in case a (synchronous) @OnChange callback invokes the getter while we're setting
        this.emitSetCachedFlag(generator, mv, false);

        // Get new value and prepare stack for PUTFIELD
        mv.visitVarInsn(Opcodes.ALOAD, 0);
        mv.visitVarInsn(Type.getType(propertyType).getOpcode(Opcodes.ILOAD), 1);

        // Cast value to the appropriate type if needed; this is required when overriding a supertype setter with generic parameter
        if (!this.typeToken.isPrimitive()) {
            final Class<?> parameterType = this.setter.getParameterTypes()[0];
            if (!propertyType.isAssignableFrom(parameterType))
                mv.visitTypeInsn(Opcodes.CHECKCAST, Type.getInternalName(propertyType));
        }

        // Wrap (if necessary) and write the value to the field
        mv.visitInsn(wide ? Opcodes.DUP2 : Opcodes.DUP);
        if (this.typeToken.isPrimitive())
            generator.wrap(mv, Primitive.get(propertyType));
        this.outputWriteCoreValueBytecode(generator, mv);

        // Now cache the value - only after a successful write
        mv.visitFieldInsn(Opcodes.PUTFIELD, className, this.getCachedValueFieldName(), Type.getDescriptor(propertyType));

        // Set the flag indicating cached value is valid
        this.emitSetCachedFlag(generator, mv, true);

        // Done with setter
        mv.visitInsn(Opcodes.RETURN);
        mv.visitMaxs(0, 0);
        mv.visitEnd();
    }

    // Get the 'cached' flag for this field onto the stack
    private void emitGetCachedFlag(ClassGenerator<?> generator, MethodVisitor mv) {
        final String className = generator.getClassName();
        final String fieldName = generator.getCachedFlagFieldName(this);
        final int flagBit = generator.getCachedFlagBit(this);
        mv.visitVarInsn(Opcodes.ALOAD, 0);
        mv.visitFieldInsn(Opcodes.GETFIELD, className, fieldName, Type.getDescriptor(generator.getCachedFlagEncoding(this)));
        switch (flagBit) {
        case 1:
            mv.visitInsn(Opcodes.ICONST_1);
            break;
        case 2:
            mv.visitInsn(Opcodes.ICONST_2);
            break;
        case 4:
            mv.visitInsn(Opcodes.ICONST_4);
            break;
        default:
            mv.visitLdcInsn(flagBit);
            break;
        }
        mv.visitInsn(Opcodes.IAND);
    }

    // Set/reset the 'cached' flag for this field
    private void emitSetCachedFlag(ClassGenerator<?> generator, MethodVisitor mv, boolean set) {
        final String className = generator.getClassName();
        final String fieldName = generator.getCachedFlagFieldName(this);
        final int flagBit = generator.getCachedFlagBit(this);
        mv.visitVarInsn(Opcodes.ALOAD, 0);
        mv.visitInsn(Opcodes.DUP);
        mv.visitFieldInsn(Opcodes.GETFIELD, className, fieldName, Type.getDescriptor(generator.getCachedFlagEncoding(this)));
        if (set) {
            switch (flagBit) {
            case 1:
                mv.visitInsn(Opcodes.ICONST_1);
                break;
            case 2:
                mv.visitInsn(Opcodes.ICONST_2);
                break;
            case 4:
                mv.visitInsn(Opcodes.ICONST_4);
                break;
            default:
                mv.visitLdcInsn(flagBit);
                break;
            }
            mv.visitInsn(Opcodes.IOR);
        } else {
            mv.visitLdcInsn(~flagBit);
            mv.visitInsn(Opcodes.IAND);
        }
        mv.visitFieldInsn(Opcodes.PUTFIELD, className, fieldName, Type.getDescriptor(generator.getCachedFlagEncoding(this)));
    }

    void outputReadCoreValueBytecode(ClassGenerator<?> generator, MethodVisitor mv) {

        // this.$tx.getTransaction().readSimpleField(this.id, STORAGEID, true)
        mv.visitVarInsn(Opcodes.ALOAD, 0);
        mv.visitFieldInsn(Opcodes.GETFIELD, generator.getClassName(),
          ClassGenerator.TX_FIELD_NAME, Type.getDescriptor(JTransaction.class));
        generator.emitInvoke(mv, ClassGenerator.JTRANSACTION_GET_TRANSACTION_METHOD);
        mv.visitVarInsn(Opcodes.ALOAD, 0);
        mv.visitFieldInsn(Opcodes.GETFIELD, generator.getClassName(),
          ClassGenerator.ID_FIELD_NAME, Type.getDescriptor(ObjId.class));
        mv.visitLdcInsn(this.storageId);
        mv.visitInsn(Opcodes.ICONST_1);
        generator.emitInvoke(mv, ClassGenerator.TRANSACTION_READ_SIMPLE_FIELD_METHOD);
    }

    void outputWriteCoreValueBytecode(ClassGenerator<?> generator, MethodVisitor mv) {

        // JTransaction.registerJObject(this);
        mv.visitVarInsn(Opcodes.ALOAD, 0);
        generator.emitInvoke(mv, ClassGenerator.JTRANSACTION_REGISTER_JOBJECT_METHOD);

        // this.$tx.getTransaction().writeSimpleField(this.id, STORAGEID, STACK[0], true)
        mv.visitVarInsn(Opcodes.ALOAD, 0);
        mv.visitFieldInsn(Opcodes.GETFIELD, generator.getClassName(),
          ClassGenerator.TX_FIELD_NAME, Type.getDescriptor(JTransaction.class));
        generator.emitInvoke(mv, ClassGenerator.JTRANSACTION_GET_TRANSACTION_METHOD);
        mv.visitInsn(Opcodes.SWAP);
        mv.visitVarInsn(Opcodes.ALOAD, 0);
        mv.visitFieldInsn(Opcodes.GETFIELD, generator.getClassName(),
          ClassGenerator.ID_FIELD_NAME, Type.getDescriptor(ObjId.class));
        mv.visitInsn(Opcodes.SWAP);
        mv.visitLdcInsn(this.storageId);
        mv.visitInsn(Opcodes.SWAP);
        mv.visitInsn(Opcodes.ICONST_1);
        generator.emitInvoke(mv, ClassGenerator.TRANSACTION_WRITE_SIMPLE_FIELD_METHOD);
    }
}
