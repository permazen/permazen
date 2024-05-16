
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen;

import com.google.common.base.Converter;
import com.google.common.base.Preconditions;
import com.google.common.reflect.TypeToken;

import io.permazen.core.DeleteAction;
import io.permazen.core.ObjId;
import io.permazen.core.ReferenceEncoding;
import io.permazen.core.ReferenceField;
import io.permazen.core.Transaction;
import io.permazen.schema.ReferenceSchemaField;

import java.lang.reflect.Method;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;

import org.objectweb.asm.ClassWriter;
import org.objectweb.asm.Label;
import org.objectweb.asm.MethodVisitor;
import org.objectweb.asm.Opcodes;
import org.objectweb.asm.Type;

/**
 * Represents a reference field in a {@link PermazenClass} or a reference sub-field of a complex field in a {@link PermazenClass}.
 */
public class PermazenReferenceField extends PermazenSimpleField {

    final DeleteAction inverseDelete;
    final boolean forwardDelete;
    final boolean allowDeleted;
    final String[] forwardCascades;
    final String[] inverseCascades;
    final Set<PermazenClass<?>> objectTypes;

// Constructor

    PermazenReferenceField(String name, int storageId, String description, TypeToken<?> typeToken,
      io.permazen.annotation.PermazenField annotation, Collection<PermazenClass<?>> pclasses, Method getter, Method setter) {
        super(name, storageId, typeToken, new ReferenceEncoding(), true, annotation, description, getter, setter);
        this.inverseDelete = annotation.inverseDelete();
        this.forwardDelete = annotation.forwardDelete();
        this.allowDeleted = annotation.allowDeleted();
        this.forwardCascades = annotation.forwardCascades();
        this.inverseCascades = annotation.inverseCascades();

        // Calculate allowed object types
        final Class<?> rawType = this.typeToken.getRawType();
        assert !UntypedPermazenObject.class.isAssignableFrom(rawType);
        if (rawType.isAssignableFrom(PermazenObject.class))
            this.objectTypes = null;
        else {
            assert !rawType.isAssignableFrom(UntypedPermazenObject.class);
            this.objectTypes = new HashSet<>();
            for (PermazenClass<?> pclass : pclasses) {
                if (rawType.isAssignableFrom(pclass.type))
                    this.objectTypes.add(pclass);
            }
        }
    }

// Public Methods

    @Override
    public PermazenObject getValue(PermazenObject pobj) {
        return (PermazenObject)super.getValue(pobj);
    }

    @Override
    public <R> R visit(PermazenFieldSwitch<R> target) {
        Preconditions.checkArgument(target != null, "null target");
        return target.casePermazenReferenceField(this);
    }

    @Override
    public Converter<ObjId, PermazenObject> getConverter(PermazenTransaction ptx) {
        return ptx.referenceConverter.reverse();
    }

    /**
     * Get the inverse {@link DeleteAction} configured for this field.
     *
     * @return this field's {@link DeleteAction}
     */
    public DeleteAction getInverseDelete() {
        return this.inverseDelete;
    }

    /**
     * Determine whether the referred-to object should be deleted when an object containing this field is deleted.
     *
     * @return this field's delete cascade setting
     */
    public boolean isForwardDelete() {
        return this.forwardDelete;
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
     * Get this field's forward copy/find cascades.
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
     * Get this field's inverse copy/find cascades.
     *
     * <p>
     * The returned array is a copy; modifications to it have no effect.
     *
     * @return zero or more inverse copy cascade names
     */
    public String[] getInverseCascades() {
        return this.inverseCascades.clone();
    }

    /**
     * Get the database object types which this field is allowed to reference.
     *
     * @return allowed object types to reference, or null if there is no restriction
     */
    public Set<PermazenClass<?>> getObjectTypes() {
        return this.objectTypes != null ? Collections.unmodifiableSet(this.objectTypes) : null;
    }

    @Override
    public ReferenceField getSchemaItem() {
        return (ReferenceField)super.getSchemaItem();
    }

// Package Methods

    @Override
    boolean isSameAs(PermazenField that0) {
        if (!super.isSameAs(that0))
            return false;
        final PermazenReferenceField that = (PermazenReferenceField)that0;
        if (!this.inverseDelete.equals(that.inverseDelete))
            return false;
        if (this.forwardDelete != that.forwardDelete)
            return false;
        if (this.allowDeleted != that.allowDeleted)
            return false;
        if (!Set.of(this.forwardCascades).equals(Set.of(that.forwardCascades)))
            return false;
        if (!Set.of(this.inverseCascades).equals(Set.of(that.inverseCascades)))
            return false;
        if (!Objects.equals(this.objectTypes, that.objectTypes))
            return false;
        return true;
    }

    @Override
    ReferenceSchemaField toSchemaItem() {
        final ReferenceSchemaField schemaField = (ReferenceSchemaField)super.toSchemaItem();
        schemaField.setInverseDelete(this.inverseDelete);
        schemaField.setForwardDelete(this.forwardDelete);
        schemaField.setAllowDeleted(this.allowDeleted);
        if (this.objectTypes != null) {
            schemaField.setObjectTypes(
              this.objectTypes.stream()
                .map(PermazenClass::getName)
                .collect(Collectors.toCollection(TreeSet::new)));
        }
        return schemaField;
    }

    @Override
    ReferenceSchemaField createSchemaItem() {
        return new ReferenceSchemaField();
    }

    // Iterate all of the values in this field in the given object, even if this is a sub-field
    Iterable<ObjId> iterateReferences(Transaction tx, ObjId id) {
        return this.isSubField() ?
          this.getParentField().iterateReferences(tx, id, this) :
          Collections.singleton((ObjId)tx.readSimpleField(id, this.name, false));
    }

// POJO import/export

    @Override
    Object importCoreValue(ImportContext context, Object value) {
        return value != null ? context.doImportPlain(value) : null;
    }

    @Override
    Object exportCoreValue(ExportContext context, Object value) {
        return value != null ? context.doExportPlain((ObjId)value) : null;
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
          ClassGenerator.TX_FIELD_NAME, Type.getDescriptor(PermazenTransaction.class));
        mv.visitInsn(Opcodes.SWAP);
        generator.emitInvoke(mv, ClassGenerator.PERMAZEN_TRANSACTION_GET_METHOD);
        mv.visitTypeInsn(Opcodes.CHECKCAST, Type.getInternalName(this.getter.getReturnType()));
        mv.visitInsn(Opcodes.ARETURN);
        mv.visitMaxs(0, 0);
        mv.visitEnd();

        //
        // Setter:
        //
        //  ObjId id;
        //  if (pobj == null)
        //      id = null;
        //  else
        //      id = pobj.getObjId();
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
        generator.emitInvoke(mv, ClassGenerator.PERMAZEN_OBJECT_GET_OBJ_ID_METHOD);
        mv.visitLabel(label3);
        mv.visitFrame(Opcodes.F_SAME1, 0, new Object[0], 1, new String[] { Type.getInternalName(ObjId.class) });
        //mv.visitTypeInsn(Opcodes.CHECKCAST, Type.getInternalName(ObjId.class));
        this.outputWriteCoreValueBytecode(generator, mv);
        mv.visitInsn(Opcodes.RETURN);
        mv.visitMaxs(0, 0);
        mv.visitEnd();
    }
}
