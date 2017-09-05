
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen;

import com.google.common.base.Converter;
import com.google.common.reflect.TypeToken;

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.HashSet;
import java.util.TreeSet;
import java.util.stream.Collectors;

import io.permazen.core.DeleteAction;
import io.permazen.core.ObjId;
import io.permazen.core.type.ReferenceFieldType;
import io.permazen.schema.ReferenceSchemaField;
import org.objectweb.asm.ClassWriter;
import org.objectweb.asm.Label;
import org.objectweb.asm.MethodVisitor;
import org.objectweb.asm.Opcodes;
import org.objectweb.asm.Type;

/**
 * Represents a reference field in a {@link JClass} or a reference sub-field of a complex field in a {@link JClass}.
 */
public class JReferenceField extends JSimpleField {

    final DeleteAction onDelete;
    final boolean cascadeDelete;
    final boolean allowDeleted;
    final boolean allowDeletedSnapshot;
    final String[] forwardCascades;
    final String[] inverseCascades;

    JReferenceField(JSimpleDB jdb, String name, int storageId, String description, TypeToken<?> typeToken,
      io.permazen.annotation.JField annotation, Method getter, Method setter) {
        super(jdb, name, storageId, typeToken, new ReferenceFieldType(), true, annotation, description, getter, setter);
        this.onDelete = annotation.onDelete();
        this.cascadeDelete = annotation.cascadeDelete();
        this.allowDeleted = annotation.allowDeleted();
        this.allowDeletedSnapshot = annotation.allowDeletedSnapshot();
        this.forwardCascades = annotation.cascades();
        this.inverseCascades = annotation.inverseCascades();
    }

    @Override
    public JObject getValue(JObject jobj) {
        return (JObject)super.getValue(jobj);
    }

    @Override
    public <R> R visit(JFieldSwitch<R> target) {
        return target.caseJReferenceField(this);
    }

    @Override
    public Converter<ObjId, JObject> getConverter(JTransaction jtx) {
        return jtx.referenceConverter.reverse();
    }

    /**
     * Get the {@link DeleteAction} configured for this field.
     *
     * @return this field's {@link DeleteAction}
     */
    public DeleteAction getOnDelete() {
        return this.onDelete;
    }

    /**
     * Determine whether the referred-to object should be deleted when an object containing this field is deleted.
     *
     * @return this field's delete cascade setting
     */
    public boolean isCascadeDelete() {
        return this.cascadeDelete;
    }

    /**
     * Determine whether this field allows assignment to deleted objects in normal transactions.
     *
     * @return this field's deleted assignment setting for normal transactions
     */
    public boolean isAllowDeleted() {
        return this.allowDeleted;
    }

    /**
     * Determine whether this field allows assignment to deleted objects in snapshot transactions.
     *
     * @return this field's deleted assignment setting for snapshot transactions
     */
    public boolean isAllowDeletedSnapshot() {
        return this.allowDeletedSnapshot;
    }

    /**
     * Get this field's forward copy cascades.
     *
     * <p>
     * The returned array is a copy; modifications to it have no effect.
     *
     * @return zero or more forward copy cascade names
     */
    public String[] getForwardCascades() {
        return this.forwardCascades.clone();
    }

    /**
     * Get this field's inverse copy cascades.
     *
     * <p>
     * The returned array is a copy; modifications to it have no effect.
     *
     * @return zero or more inverse copy cascade names
     */
    public String[] getInverseCascades() {
        return this.inverseCascades.clone();
    }

    @Override
    boolean isSameAs(JField that0) {
        if (!super.isSameAs(that0))
            return false;
        final JReferenceField that = (JReferenceField)that0;
        if (!this.onDelete.equals(that.onDelete))
            return false;
        if (this.cascadeDelete != that.cascadeDelete)
            return false;
        if (this.allowDeleted != that.allowDeleted)
            return false;
        if (this.allowDeletedSnapshot != that.allowDeletedSnapshot)
            return false;
        if (!new HashSet<>(Arrays.asList(this.forwardCascades)).equals(new HashSet<>(Arrays.asList(that.forwardCascades))))
            return false;
        if (!new HashSet<>(Arrays.asList(this.inverseCascades)).equals(new HashSet<>(Arrays.asList(that.inverseCascades))))
            return false;
        return true;
    }

    @Override
    ReferenceSchemaField toSchemaItem(JSimpleDB jdb) {
        final ReferenceSchemaField schemaField = new ReferenceSchemaField();
        super.initialize(jdb, schemaField);
        schemaField.setOnDelete(this.onDelete);
        schemaField.setCascadeDelete(this.cascadeDelete);
        schemaField.setAllowDeleted(this.allowDeleted);
        schemaField.setAllowDeletedSnapshot(this.allowDeletedSnapshot);
        final Class<?> rawType = this.typeToken.getRawType();
        if (!rawType.isAssignableFrom(JObject.class)) {
            assert !rawType.isAssignableFrom(UntypedJObject.class);
            if (UntypedJObject.class.isAssignableFrom(rawType))
                throw new RuntimeException("internal error: " + rawType);
            schemaField.setObjectTypes(
              jdb.getJClasses(rawType).stream()
               .map(jclass -> jclass.storageId)
               .collect(Collectors.toCollection(TreeSet::new)));
        }
        return schemaField;
    }

// Bytecode generation

    @Override
    void outputMethods(final ClassGenerator<?> generator, ClassWriter cw) {

        //
        // Getter:
        //
        //  Object value = this.readCoreAPIValue();
        //  if (value == null)
        //      return null;
        //  ObjId id = (ObjId)value;
        //  return this.tx.get(id);
        //
        MethodVisitor mv = cw.visitMethod(
          this.getter.getModifiers() & (Opcodes.ACC_PUBLIC | Opcodes.ACC_PROTECTED),
          this.getter.getName(), Type.getMethodDescriptor(this.getter), null, generator.getExceptionNames(this.getter));
        this.outputReadCoreValueBytecode(generator, mv);
        mv.visitInsn(Opcodes.DUP);
        final Label label1 = new Label();
        mv.visitJumpInsn(Opcodes.IFNONNULL, label1);
        mv.visitInsn(Opcodes.POP);
        mv.visitInsn(Opcodes.ACONST_NULL);
        mv.visitInsn(Opcodes.ARETURN);
        mv.visitLabel(label1);
        mv.visitFrame(Opcodes.F_SAME1, 0, new Object[0], 1, new String[] { Type.getInternalName(Object.class) });
        mv.visitTypeInsn(Opcodes.CHECKCAST, Type.getInternalName(ObjId.class));
        mv.visitVarInsn(Opcodes.ALOAD, 0);
        mv.visitFieldInsn(Opcodes.GETFIELD, generator.getClassName(),
          ClassGenerator.TX_FIELD_NAME, Type.getDescriptor(JTransaction.class));
        mv.visitInsn(Opcodes.SWAP);
        generator.emitInvoke(mv, ClassGenerator.JTRANSACTION_GET_METHOD);
        mv.visitTypeInsn(Opcodes.CHECKCAST, Type.getInternalName(this.getter.getReturnType()));
        mv.visitInsn(Opcodes.ARETURN);
        mv.visitMaxs(0, 0);
        mv.visitEnd();

        //
        // Setter:
        //
        //  ObjId id;
        //  if (jobj == null)
        //      id = null;
        //  else
        //      id = jobj.getObjId();
        //  this.writeCoreAPIValue(id);
        //
        mv = cw.visitMethod(
          this.setter.getModifiers() & (Opcodes.ACC_PUBLIC | Opcodes.ACC_PROTECTED),
          this.setter.getName(), Type.getMethodDescriptor(this.setter), null, generator.getExceptionNames(this.setter));
        mv.visitVarInsn(Opcodes.ALOAD, 1);
        final Label label2 = new Label();
        final Label label3 = new Label();
        mv.visitJumpInsn(Opcodes.IFNONNULL, label2);
        mv.visitInsn(Opcodes.ACONST_NULL);
        mv.visitJumpInsn(Opcodes.GOTO, label3);
        mv.visitLabel(label2);
        mv.visitFrame(Opcodes.F_SAME, 0, new Object[0], 0, new Object[0]);
        mv.visitVarInsn(Opcodes.ALOAD, 1);
        generator.emitInvoke(mv, ClassGenerator.JOBJECT_GET_OBJ_ID_METHOD);
        mv.visitLabel(label3);
        mv.visitFrame(Opcodes.F_SAME1, 0, new Object[0], 1, new String[] { Type.getInternalName(ObjId.class) });
        //mv.visitTypeInsn(Opcodes.CHECKCAST, Type.getInternalName(ObjId.class));
        this.outputWriteCoreValueBytecode(generator, mv);
        mv.visitInsn(Opcodes.RETURN);
        mv.visitMaxs(0, 0);
        mv.visitEnd();
    }
}

