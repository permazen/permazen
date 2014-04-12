
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb;

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.ArrayList;

import org.dellroad.stuff.java.Primitive;
import org.jsimpledb.core.DatabaseException;
import org.jsimpledb.core.ObjId;
import org.objectweb.asm.ClassWriter;
import org.objectweb.asm.FieldVisitor;
import org.objectweb.asm.MethodVisitor;
import org.objectweb.asm.Opcodes;
import org.objectweb.asm.Type;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Generates JSimpleDB model classes.
 *
 * <p>
 * The generated classes are subclasses of the the user-provided Java object classes (typically these are abstract classes).
 * The various annotated getter and setter methods will be overridden in the generated class.
 * If the user class implements {@link JObject}, then those methods will also be overridden with concrete implementations
 * in the generated class.
 * </p>
 */
class ClassGenerator<T> {

     // String to use for the "source file" attribute in generated classes.
    static final String GEN_SOURCE = "[GeneratedByJSimpleDB]";

     // Class name suffix for generated classes
    static final String CLASSNAME_SUFFIX = "$$JSimpleDB";

    // Names of generated fields
    static final String ID_FIELD_NAME = "$id";

    // Names of generated methods
    static final String GET_TX_METHOD_NAME = "$getTx";

    // JObject method handles
    static final Method GET_OBJ_ID_METHOD;
    static final Method JOBJECT_DELETE_METHOD;
    static final Method JOBJECT_EXISTS_METHOD;
    static final Method JOBJECT_RECREATE_METHOD;
    static final Method JOBJECT_UPGRADE_METHOD;
    static final Method JOBJECT_REVALIDATE_METHOD;

    // JTransaction method handles
    static final Method GET_TRANSACTION_METHOD;
    static final Method GET_CURRENT_METHOD;
    static final Method READ_SIMPLE_FIELD_METHOD;
    static final Method WRITE_SIMPLE_FIELD_METHOD;
    static final Method READ_SET_FIELD_METHOD;
    static final Method READ_LIST_FIELD_METHOD;
    static final Method READ_MAP_FIELD_METHOD;
    static final Method DELETE_METHOD;
    static final Method EXISTS_METHOD;
    static final Method RECREATE_METHOD;
    static final Method UPDATE_SCHEMA_VERSION_METHOD;
    static final Method REVALIDATE_METHOD;
    static final Method QUERY_SIMPLE_FIELD_METHOD;
    static final Method QUERY_LIST_FIELD_ENTRIES_METHOD;
    static final Method QUERY_MAP_FIELD_KEY_ENTRIES_METHOD;
    static final Method QUERY_MAP_FIELD_VALUE_ENTRIES_METHOD;

    static {
        try {

            // JObject methods
            GET_OBJ_ID_METHOD = JObject.class.getMethod("getObjId");
            JOBJECT_DELETE_METHOD = JObject.class.getMethod("delete");
            JOBJECT_EXISTS_METHOD = JObject.class.getMethod("exists");
            JOBJECT_RECREATE_METHOD = JObject.class.getMethod("recreate");
            JOBJECT_UPGRADE_METHOD = JObject.class.getMethod("upgrade");
            JOBJECT_REVALIDATE_METHOD = JObject.class.getMethod("revalidate");

            // JTransaction methods
            GET_TRANSACTION_METHOD = JTransaction.class.getMethod("getTransaction");
            GET_CURRENT_METHOD = JTransaction.class.getMethod("getCurrent");
            READ_SIMPLE_FIELD_METHOD = JTransaction.class.getMethod("readSimpleField", ObjId.class, Integer.TYPE);
            WRITE_SIMPLE_FIELD_METHOD = JTransaction.class.getMethod("writeSimpleField", ObjId.class, Integer.TYPE, Object.class);
            READ_SET_FIELD_METHOD = JTransaction.class.getMethod("readSetField", ObjId.class, Integer.TYPE);
            READ_LIST_FIELD_METHOD = JTransaction.class.getMethod("readListField", ObjId.class, Integer.TYPE);
            READ_MAP_FIELD_METHOD = JTransaction.class.getMethod("readMapField", ObjId.class, Integer.TYPE);
            DELETE_METHOD = JTransaction.class.getMethod("delete", JObject.class);
            EXISTS_METHOD = JTransaction.class.getMethod("exists", JObject.class);
            RECREATE_METHOD = JTransaction.class.getMethod("recreate", JObject.class);
            UPDATE_SCHEMA_VERSION_METHOD = JTransaction.class.getMethod("updateSchemaVersion", JObject.class);
            REVALIDATE_METHOD = JTransaction.class.getMethod("revalidate", JObject.class);
            QUERY_SIMPLE_FIELD_METHOD = JTransaction.class.getMethod("querySimpleField", int.class);
            QUERY_LIST_FIELD_ENTRIES_METHOD = JTransaction.class.getMethod("queryListFieldEntries", int.class);
            QUERY_MAP_FIELD_KEY_ENTRIES_METHOD = JTransaction.class.getMethod("queryMapFieldKeyEntries", int.class);
            QUERY_MAP_FIELD_VALUE_ENTRIES_METHOD = JTransaction.class.getMethod("queryMapFieldValueEntries", int.class);
        } catch (NoSuchMethodException e) {
            throw new RuntimeException("internal error", e);
        }
    }

    protected final Logger log = LoggerFactory.getLogger(this.getClass());

    final JClass<T> jclass;
    final ClassLoader loader;

    /**
     * Constructor.
     */
    public ClassGenerator(JClass<T> jclass) {
        if (jclass == null)
            throw new IllegalArgumentException("null jclass");
        this.jclass = jclass;
        this.loader = new ClassLoader(this.jclass.typeToken.getRawType().getClassLoader()) {
            @Override
            protected Class<?> findClass(String name) throws ClassNotFoundException {
                if (!name.replace('.', '/').equals(ClassGenerator.this.getClassName()))
                    return super.findClass(name);
                ClassGenerator.this.log.debug("generating class " + name);
                final byte[] bytes = ClassGenerator.this.generateBytecode();
                return this.defineClass(null, bytes, 0, bytes.length);
            }
        };
    }

    /**
     * Generate the Java class for this instance's {@link JClass}.
     */
    @SuppressWarnings("unchecked")
    public Class<? extends T> generateClass() {
        try {
            return (Class<? extends T>)this.loader.loadClass(this.getClassName());
        } catch (ClassNotFoundException e) {
            throw new DatabaseException("internal error", e);
        }
    }

    /**
     * Generate the Java class bytecode for this instance's {@link JClass}.
     */
    protected byte[] generateBytecode() {

        // Generate class
        final ClassWriter cw = new ClassWriter(ClassWriter.COMPUTE_MAXS);
        cw.visit(Opcodes.V1_6, Opcodes.ACC_PUBLIC | Opcodes.ACC_SUPER | Opcodes.ACC_FINAL | Opcodes.ACC_SYNTHETIC,
          this.getClassName(), null, this.getSuperclassName(), new String[] { Type.getInternalName(JObject.class) });
        cw.visitSource(GEN_SOURCE, null);
        this.outputFields(cw);
        this.outputConstructors(cw);
        this.outputMethods(cw);
        cw.visitEnd();
        final byte[] classfile = cw.toByteArray();

        // Debug dump
        // CHECKSTYLE OFF: GenericIllegalRegexp
        if (false) {
            System.out.println("***************** BEGIN " + this.getClassName() + " ******************");
            org.objectweb.asm.ClassReader cr = new org.objectweb.asm.ClassReader(classfile);
            java.io.PrintWriter pw = new java.io.PrintWriter(System.out, true);
            cr.accept(new org.objectweb.asm.util.TraceClassVisitor(pw), 0);
            pw.flush();
            System.out.println("***************** END " + this.getClassName() + " ******************");
        }
        // CHECKSTYLE ON: GenericIllegalRegexp

        // Done
        return classfile;
    }

    /**
     * Get class internal name.
     */
    private String getClassName() {
        return this.getSuperclassName() + CLASSNAME_SUFFIX;
    }

    /**
     * Get superclass internal name.
     */
    private String getSuperclassName() {
        return Type.getInternalName(this.jclass.typeToken.getRawType());
    }

    private void outputFields(ClassWriter cw) {

        // Output "id" field
        final FieldVisitor idField = cw.visitField(Opcodes.ACC_PRIVATE | Opcodes.ACC_FINAL,
          ID_FIELD_NAME, Type.getDescriptor(ObjId.class), null, null);
        idField.visitEnd();
    }

    private void outputConstructors(ClassWriter cw) {

        // Foo(ObjId id, Transaction tx)
        final String primaryDescriptor = Type.getMethodDescriptor(Type.VOID_TYPE, Type.getType(ObjId.class));
        MethodVisitor mv = cw.visitMethod(Opcodes.ACC_PUBLIC, "<init>", primaryDescriptor, null, null);
        mv.visitCode();
        mv.visitVarInsn(Opcodes.ALOAD, 0);                                                          // this.id = id
        mv.visitVarInsn(Opcodes.ALOAD, 1);
        mv.visitFieldInsn(Opcodes.PUTFIELD, this.getClassName(), ID_FIELD_NAME, Type.getDescriptor(ObjId.class));
        mv.visitVarInsn(Opcodes.ALOAD, 0);                                                          // this.id = id
        mv.visitMethodInsn(Opcodes.INVOKESPECIAL, this.getSuperclassName(), "<init>", "()V");
        mv.visitInsn(Opcodes.RETURN);
        mv.visitMaxs(0, 0);
        mv.visitEnd();
    }

    private void outputMethods(ClassWriter cw) {

        // Output getTx() method
        MethodVisitor mv = cw.visitMethod(Opcodes.ACC_PRIVATE, GET_TX_METHOD_NAME,
          Type.getMethodDescriptor(Type.getType(JTransaction.class)), null, null);
        mv.visitCode();
        this.emitInvoke(mv, GET_CURRENT_METHOD);                                                // JTransaction.getCurrent()
        mv.visitInsn(Opcodes.ARETURN);
        mv.visitMaxs(0, 0);
        mv.visitEnd();

        // Add JObject.getObjId()
        mv = cw.visitMethod(Opcodes.ACC_PUBLIC, GET_OBJ_ID_METHOD.getName(),
          Type.getMethodDescriptor(GET_OBJ_ID_METHOD), null, null);
        mv.visitCode();
        mv.visitVarInsn(Opcodes.ALOAD, 0);
        mv.visitFieldInsn(Opcodes.GETFIELD, this.getClassName(), ID_FIELD_NAME, Type.getDescriptor(ObjId.class));
        mv.visitInsn(Opcodes.ARETURN);
        mv.visitMaxs(0, 0);
        mv.visitEnd();

        // Add JObject.delete()
        mv = cw.visitMethod(Opcodes.ACC_PUBLIC, JOBJECT_DELETE_METHOD.getName(),
          Type.getMethodDescriptor(JOBJECT_DELETE_METHOD), null, null);
        mv.visitCode();
        mv.visitVarInsn(Opcodes.ALOAD, 0);
        mv.visitMethodInsn(Opcodes.INVOKESPECIAL, this.getClassName(), GET_TX_METHOD_NAME,
          Type.getMethodDescriptor(Type.getType(JTransaction.class)));
        mv.visitVarInsn(Opcodes.ALOAD, 0);
        this.emitInvoke(mv, DELETE_METHOD);
        mv.visitInsn(Opcodes.IRETURN);
        mv.visitMaxs(0, 0);
        mv.visitEnd();

        // Add JObject.exists()
        mv = cw.visitMethod(Opcodes.ACC_PUBLIC, JOBJECT_EXISTS_METHOD.getName(),
          Type.getMethodDescriptor(JOBJECT_EXISTS_METHOD), null, null);
        mv.visitCode();
        mv.visitVarInsn(Opcodes.ALOAD, 0);
        mv.visitMethodInsn(Opcodes.INVOKESPECIAL, this.getClassName(), GET_TX_METHOD_NAME,
          Type.getMethodDescriptor(Type.getType(JTransaction.class)));
        mv.visitVarInsn(Opcodes.ALOAD, 0);
        this.emitInvoke(mv, RECREATE_METHOD);
        mv.visitInsn(Opcodes.IRETURN);
        mv.visitMaxs(0, 0);
        mv.visitEnd();

        // Add JObject.recreate()
        mv = cw.visitMethod(Opcodes.ACC_PUBLIC, JOBJECT_RECREATE_METHOD.getName(),
          Type.getMethodDescriptor(JOBJECT_RECREATE_METHOD), null, null);
        mv.visitCode();
        mv.visitVarInsn(Opcodes.ALOAD, 0);
        mv.visitMethodInsn(Opcodes.INVOKESPECIAL, this.getClassName(), GET_TX_METHOD_NAME,
          Type.getMethodDescriptor(Type.getType(JTransaction.class)));
        mv.visitVarInsn(Opcodes.ALOAD, 0);
        this.emitInvoke(mv, RECREATE_METHOD);
        mv.visitInsn(Opcodes.IRETURN);
        mv.visitMaxs(0, 0);
        mv.visitEnd();

        // Add JObject.revalidate()
        mv = cw.visitMethod(Opcodes.ACC_PUBLIC, JOBJECT_REVALIDATE_METHOD.getName(),
          Type.getMethodDescriptor(JOBJECT_REVALIDATE_METHOD), null, null);
        mv.visitCode();
        mv.visitVarInsn(Opcodes.ALOAD, 0);
        mv.visitMethodInsn(Opcodes.INVOKESPECIAL, this.getClassName(), GET_TX_METHOD_NAME,
          Type.getMethodDescriptor(Type.getType(JTransaction.class)));
        mv.visitVarInsn(Opcodes.ALOAD, 0);
        this.emitInvoke(mv, REVALIDATE_METHOD);
        mv.visitInsn(Opcodes.RETURN);
        mv.visitMaxs(0, 0);
        mv.visitEnd();

        // Add JObject.upgrade()
        mv = cw.visitMethod(Opcodes.ACC_PUBLIC, JOBJECT_UPGRADE_METHOD.getName(),
          Type.getMethodDescriptor(JOBJECT_UPGRADE_METHOD), null, null);
        mv.visitCode();
        mv.visitVarInsn(Opcodes.ALOAD, 0);
        mv.visitMethodInsn(Opcodes.INVOKESPECIAL, this.getClassName(), GET_TX_METHOD_NAME,
          Type.getMethodDescriptor(Type.getType(JTransaction.class)));
        mv.visitVarInsn(Opcodes.ALOAD, 0);
        this.emitInvoke(mv, UPDATE_SCHEMA_VERSION_METHOD);
        mv.visitInsn(Opcodes.RETURN);
        mv.visitMaxs(0, 0);
        mv.visitEnd();

        // Add methods that override field getters & setters
        for (JField jfield : this.jclass.jfields.values())
            jfield.outputMethods(this, cw);

        // Add methods that override @IndexQuery methods
        for (IndexQueryScanner<?>.MethodInfo i : jclass.indexQueryMethods) {
            final IndexQueryScanner<?>.IndexMethodInfo info = (IndexQueryScanner<?>.IndexMethodInfo)i;
            final Method method = info.getMethod();

            // Generate initial stuff
            mv = cw.visitMethod(method.getModifiers() & (Opcodes.ACC_PUBLIC | Opcodes.ACC_PRIVATE | Opcodes.ACC_PROTECTED),
              method.getName(), Type.getMethodDescriptor(method), null, this.getExceptionNames(method));

            // Invoke this.getTx().queryXXX()
            mv.visitVarInsn(Opcodes.ALOAD, 0);
            mv.visitMethodInsn(Opcodes.INVOKESPECIAL, this.getClassName(), GET_TX_METHOD_NAME,  // Transaction tx = this.getTx()
              Type.getMethodDescriptor(Type.getType(JTransaction.class)));
            final boolean isEntryQuery = info.targetSuperField != null && info.queryType != 0;
            mv.visitLdcInsn(isEntryQuery ? info.targetSuperField.storageId : info.targetField.storageId);
            this.emitInvoke(mv, isEntryQuery ?
              info.targetSuperField.getIndexEntryQueryMethod(info.queryType) : QUERY_SIMPLE_FIELD_METHOD);
            mv.visitInsn(Opcodes.ARETURN);
            mv.visitMaxs(0, 0);
            mv.visitEnd();
        }
    }

    /**
     * Emit code that overrides a Java bean method. When the {@code emitter} runs, the stack will look like:
     * {@code ..., tx, id, storageId }.
     */
    void overrideBeanMethod(ClassWriter cw, Method method, int storageId, CodeEmitter emitter) {

        // Generate initial stuff
        final MethodVisitor mv = cw.visitMethod(
          method.getModifiers() & (Opcodes.ACC_PUBLIC | Opcodes.ACC_PRIVATE | Opcodes.ACC_PROTECTED),
          method.getName(), Type.getMethodDescriptor(method), null, this.getExceptionNames(method));
        mv.visitVarInsn(Opcodes.ALOAD, 0);
        mv.visitMethodInsn(Opcodes.INVOKESPECIAL, this.getClassName(), GET_TX_METHOD_NAME,          // Transaction tx = this.getTx()
          Type.getMethodDescriptor(Type.getType(JTransaction.class)));
        mv.visitVarInsn(Opcodes.ALOAD, 0);
        mv.visitFieldInsn(Opcodes.GETFIELD, this.getClassName(), ID_FIELD_NAME, Type.getDescriptor(ObjId.class));
        mv.visitLdcInsn(storageId);

        // Emit caller-specific stuff
        emitter.emit(mv);

        // Return
        mv.visitInsn(Type.getType(method.getReturnType()).getOpcode(Opcodes.IRETURN));

        // Finish up
        mv.visitMaxs(0, 0);
        mv.visitEnd();
    }

    /**
     * Emit code that wraps the primitive value on the top of the stack.
     */
    void wrap(MethodVisitor mv, Primitive<?> primitive) {
        final Type wrapperType = Type.getType(primitive.getWrapperType());
        mv.visitMethodInsn(Opcodes.INVOKESTATIC, wrapperType.getInternalName(), "valueOf",
          Type.getMethodDescriptor(wrapperType, Type.getType(primitive.getType())));
    }

    /**
     * Emit code that unwraps the primitive value on the top of the stack.
     */
    void unwrap(MethodVisitor mv, Primitive<?> primitive) {
        final Type wrapperType = Type.getType(primitive.getWrapperType());
        final Method unwrapMethod = primitive.getUnwrapMethod();
        this.emitInvoke(mv, unwrapMethod);
    }

    /**
     * Emit code to invoke a method. This assumes the stack is loaded.
     */
    void emitInvoke(MethodVisitor mv, Method method) {
        mv.visitMethodInsn((method.getModifiers() & Modifier.STATIC) != 0 ? Opcodes.INVOKESTATIC : Opcodes.INVOKEVIRTUAL,
          Type.getInternalName(method.getDeclaringClass()), method.getName(), Type.getMethodDescriptor(method));
    }

    private String[] getExceptionNames(Method method) {
        ArrayList<String> list = new ArrayList<String>();
        for (Class<?> type : method.getExceptionTypes())
            list.add(Type.getType(type).getInternalName());
        return list.toArray(new String[list.size()]);
    }

    // Callback interface for emitting bytecode
    interface CodeEmitter {

        /**
         * Output some method bytecode or whatever.
         */
        void emit(MethodVisitor mv);
    }
}

