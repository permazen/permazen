
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen;

import com.google.common.base.Converter;
import com.google.common.base.Preconditions;

import io.permazen.core.DatabaseException;
import io.permazen.core.ObjId;
import io.permazen.core.Transaction;
import io.permazen.core.util.ObjDumper;

import java.io.PrintStream;
import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.SortedSet;
import java.util.stream.Stream;

import org.dellroad.stuff.java.Primitive;
import org.objectweb.asm.ClassWriter;
import org.objectweb.asm.FieldVisitor;
import org.objectweb.asm.Label;
import org.objectweb.asm.MethodVisitor;
import org.objectweb.asm.Opcodes;
import org.objectweb.asm.Type;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Generates Permazen model classes.
 *
 * <p>
 * The generated classes are subclasses of the the user-provided Java object classes (typically these are abstract classes).
 * The various annotated getter and setter methods will be overridden in the generated class.
 * If the user class implements {@link PermazenObject}, then those methods will also be overridden with concrete implementations
 * in the generated class.
 */
class ClassGenerator<T> {

     // String to use for the "source file" attribute in generated classes.
    static final String GEN_SOURCE = "[GeneratedByPermazen]";

    // Names of generated fields
    static final String TX_FIELD_NAME = "$tx";
    static final String ID_FIELD_NAME = "$id";
    static final String CACHED_VALUE_FIELD_PREFIX = "$cached_";
    static final String CACHED_FLAG_FIELD_PREFIX = "$cacheflags";
    static final String CONVERTER_FIELD_PREFIX = "$converter";
    static final String REFERENCE_PATH_FIELD_PREFIX = "$referencePath";

    // PermazenObject method handles
    static final Method PERMAZEN_OBJECT_GET_OBJ_ID_METHOD;
    static final Method PERMAZEN_OBJECT_GET_PERMAZEN_TRANSACTION_METHOD;
    static final Method PERMAZEN_OBJECT_GET_MODEL_CLASS_METHOD;
    static final Method PERMAZEN_OBJECT_RESET_CACHED_FIELD_VALUES_METHOD;

    // PermazenTransaction method handles
    static final Method PERMAZEN_TRANSACTION_READ_COUNTER_FIELD_METHOD;
    static final Method PERMAZEN_TRANSACTION_READ_SET_FIELD_METHOD;
    static final Method PERMAZEN_TRANSACTION_READ_LIST_FIELD_METHOD;
    static final Method PERMAZEN_TRANSACTION_READ_MAP_FIELD_METHOD;
    static final Method PERMAZEN_TRANSACTION_GET_TRANSACTION_METHOD;
    static final Method PERMAZEN_TRANSACTION_GET_METHOD;
    static final Method PERMAZEN_TRANSACTION_REGISTER_PERMAZEN_OBJECT_METHOD;
    static final Method PERMAZEN_TRANSACTION_GET_PERMAZEN_METHOD;
    static final Method PERMAZEN_TRANSACTION_FOLLOW_REFERENCE_PATH_METHOD;

    // Permazen method handles
    static final Method PERMAZEN_PARSE_REFERENCE_PATH_METHOD;

    // Converter method handles
    static final Method CONVERTER_CONVERT_METHOD;
    static final Method CONVERTER_REVERSE_METHOD;

    // EnumConverter method handles
    static final Method ENUM_CONVERTER_CREATE_METHOD;
    static final Method ENUM_CONVERTER_CREATE_ARRAY_METHOD;

    // Transaction method handles
    static final Method TRANSACTION_READ_SIMPLE_FIELD_METHOD;
    static final Method TRANSACTION_WRITE_SIMPLE_FIELD_METHOD;

    // ObjDumper method handles
    static final Method OBJ_DUMPER_TO_STRING_METHOD;

    // Collections method handles
    static final Method SORTED_SET_FIRST_METHOD;

    // Optional method handles
    static final Method OPTIONAL_OF_METHOD;
    static final Method OPTIONAL_EMPTY_METHOD;

    // Object constructor and method handles
    static final Constructor<Object> OBJECT_CONSTRUCTOR;
    static final Method OBJECT_GET_CLASS_METHOD;
    static final Method OBJECT_TO_STRING_METHOD;

    // Class method handles
    static final Method CLASS_GET_SUPERCLASS_METHOD;

    // Util method handles
    static final Method UTIL_STREAM_OF_METHOD;

    // Max number of collection entries for ObjDumper.toString()
    private static final int TO_STRING_MAX_COLLECTION_ENTRIES = 16;

    static {
        try {

            // PermazenObject methods
            PERMAZEN_OBJECT_GET_OBJ_ID_METHOD = PermazenObject.class.getMethod("getObjId");
            PERMAZEN_OBJECT_GET_PERMAZEN_TRANSACTION_METHOD = PermazenObject.class.getMethod("getPermazenTransaction");
            PERMAZEN_OBJECT_GET_MODEL_CLASS_METHOD = PermazenObject.class.getMethod("getModelClass");
            PERMAZEN_OBJECT_RESET_CACHED_FIELD_VALUES_METHOD = PermazenObject.class.getMethod("resetCachedFieldValues");

            // PermazenTransaction methods
            PERMAZEN_TRANSACTION_READ_COUNTER_FIELD_METHOD = PermazenTransaction.class.getMethod("readCounterField",
              ObjId.class, String.class, boolean.class);
            PERMAZEN_TRANSACTION_READ_SET_FIELD_METHOD = PermazenTransaction.class.getMethod("readSetField",
              ObjId.class, String.class, boolean.class);
            PERMAZEN_TRANSACTION_READ_LIST_FIELD_METHOD = PermazenTransaction.class.getMethod("readListField",
              ObjId.class, String.class, boolean.class);
            PERMAZEN_TRANSACTION_READ_MAP_FIELD_METHOD = PermazenTransaction.class.getMethod("readMapField",
              ObjId.class, String.class, boolean.class);
            PERMAZEN_TRANSACTION_GET_TRANSACTION_METHOD = PermazenTransaction.class.getMethod("getTransaction");
            PERMAZEN_TRANSACTION_GET_METHOD = PermazenTransaction.class.getMethod("get", ObjId.class);
            PERMAZEN_TRANSACTION_REGISTER_PERMAZEN_OBJECT_METHOD = PermazenTransaction.class.getMethod("registerPermazenObject",
              PermazenObject.class);
            PERMAZEN_TRANSACTION_GET_PERMAZEN_METHOD = PermazenTransaction.class.getMethod("getPermazen");
            PERMAZEN_TRANSACTION_FOLLOW_REFERENCE_PATH_METHOD = PermazenTransaction.class.getMethod("followReferencePath",
              ReferencePath.class, Stream.class);

            // Permazen methods
            PERMAZEN_PARSE_REFERENCE_PATH_METHOD = Permazen.class.getMethod("parseReferencePath", Class.class, String.class);

            // Transaction methods
            TRANSACTION_READ_SIMPLE_FIELD_METHOD = Transaction.class.getMethod("readSimpleField",
              ObjId.class, String.class, boolean.class);
            TRANSACTION_WRITE_SIMPLE_FIELD_METHOD = Transaction.class.getMethod("writeSimpleField",
              ObjId.class, String.class, Object.class, boolean.class);

            // Converter
            CONVERTER_CONVERT_METHOD = Converter.class.getMethod("convert", Object.class);
            CONVERTER_REVERSE_METHOD = Converter.class.getMethod("reverse");

            // EnumConverter
            ENUM_CONVERTER_CREATE_METHOD = EnumConverter.class.getMethod("createEnumConverter", Class.class);
            ENUM_CONVERTER_CREATE_ARRAY_METHOD = EnumConverter.class.getMethod("createEnumArrayConverter", Class.class, int.class);

            // ObjDumper
            OBJ_DUMPER_TO_STRING_METHOD = ObjDumper.class.getMethod("toString", Transaction.class, ObjId.class, int.class);

            // Optional
            OPTIONAL_OF_METHOD = Optional.class.getMethod("of", Object.class);
            OPTIONAL_EMPTY_METHOD = Optional.class.getMethod("empty");

            // Collections
            SORTED_SET_FIRST_METHOD = SortedSet.class.getMethod("first");

            // Util
            UTIL_STREAM_OF_METHOD = Util.class.getMethod("streamOf", Object.class);

            // Object
            OBJECT_CONSTRUCTOR = Object.class.getConstructor();
            OBJECT_GET_CLASS_METHOD = Object.class.getMethod("getClass");
            OBJECT_TO_STRING_METHOD = Object.class.getMethod("toString");

            // Class
            CLASS_GET_SUPERCLASS_METHOD = Class.class.getMethod("getSuperclass");
        } catch (NoSuchMethodException e) {
            throw new RuntimeException("internal error", e);
        }
    }

    protected final Logger log = LoggerFactory.getLogger(this.getClass());

    protected final Permazen pdb;
    protected final PermazenClass<T> pclass;
    protected final Class<T> modelClass;

    private final ArrayList<String> simpleFieldNames = new ArrayList<>();

    private Class<? extends T> subclass;
    private Constructor<? extends T> constructor;
    private Constructor<? super T> superclassConstructor;

    /**
     * Constructor for application classes.
     */
    ClassGenerator(Permazen pdb, PermazenClass<T> pclass) {
        this(pdb, pclass, pclass.type);
    }

    /**
     * Constructor for a "PermazenObject" class with no fields.
     */
    ClassGenerator(Permazen pdb, Class<T> modelClass) {
        this(pdb, null, modelClass);
    }

    /**
     * Internal constructor.
     */
    private ClassGenerator(Permazen pdb, PermazenClass<T> pclass, Class<T> modelClass) {
        this.pdb = pdb;
        this.pclass = pclass;
        this.modelClass = modelClass;

        // Use superclass constructor taking either (a) (PermazenTransaction tx, ObjId id) or (b) no parameters
        if (this.modelClass.isInterface())
            this.superclassConstructor = OBJECT_CONSTRUCTOR;
        else {
            try {
                this.superclassConstructor = this.modelClass.getDeclaredConstructor(PermazenTransaction.class, ObjId.class);
            } catch (NoSuchMethodException e) {
                try {
                    this.superclassConstructor = this.modelClass.getDeclaredConstructor();
                } catch (NoSuchMethodException e2) {
                    String message = String.format("no suitable constructor found in model class %s; model classes must have"
                      + " a public or protected constructor taking either () or (%s, %s)",
                      this.modelClass.getName(), PermazenTransaction.class.getSimpleName(), ObjId.class.getSimpleName());
                    if (this.modelClass.isMemberClass() && !Modifier.isStatic(this.modelClass.getModifiers()))
                        message += "; did you mean to declare this class \"static\"?";
                    throw new IllegalArgumentException(message);
                }
            }
        }

        // Verify superclass constructor is accessible
        if ((this.superclassConstructor.getModifiers() & (Modifier.PUBLIC | Modifier.PROTECTED)) == 0) {
            throw new IllegalArgumentException(String.format(
              "model class %s constructor %s is inaccessible; must be either public or protected",
              this.modelClass.getName(), this.superclassConstructor));
        }
    }

    /**
     * Get generated subclass' constructor.
     */
    public Constructor<? extends T> getConstructor() {
        if (this.constructor == null) {
            if (this.subclass == null)
                this.subclass = this.generateClass();
            try {
                this.constructor = this.subclass.getConstructor(PermazenTransaction.class, ObjId.class);
            } catch (NoSuchMethodException e) {
                throw new RuntimeException("internal error", e);
            }
            this.constructor.setAccessible(true);
        }
        return this.constructor;
    }

    /**
     * Generate the Java class for this instance's {@link PermazenClass}.
     *
     * <p>
     * This method also initializes the class to force early detection of any bytecode verification errors.
     */
    @SuppressWarnings("unchecked")
    public Class<? extends T> generateClass() {

        // Gather simple field names (non sub-field only)
        if (this.pclass != null) {
            for (PermazenSimpleField pfield : this.pclass.simpleFieldsByName.values()) {
                if (!pfield.isSubField())
                    this.simpleFieldNames.add(pfield.name);
            }
        }

        // Load class to generate it
        try {
            return (Class<? extends T>)Class.forName(this.getClassName().replace('/', '.'), true, this.pdb.loader);
        } catch (ClassNotFoundException e) {
            throw new DatabaseException("internal error", e);
        }
    }

    /**
     * Get class internal name. Note: this name contains slashes, not dots.
     */
    public String getClassName() {
        return Type.getInternalName(this.modelClass) + Permazen.GENERATED_CLASS_NAME_SUFFIX;
    }

    /**
     * Get superclass (i.e., original Java model class) internal name.
     */
    public String getSuperclassName() {
        return Type.getInternalName(this.modelClass.isInterface() ? Object.class : this.modelClass);
    }

// Database class

    /**
     * Generate the Java class bytecode for this instance's {@link PermazenClass}.
     */
    protected byte[] generateBytecode() {

        // Generate class
        if (this.log.isTraceEnabled())
            this.log.trace("begin generating class {}", this.getClassName());
        final ClassWriter cw = new ClassWriter(ClassWriter.COMPUTE_MAXS);
        final String[] interfaces = this.modelClass.isInterface() ?
          new String[] { Type.getInternalName(this.modelClass), Type.getInternalName(PermazenObject.class) } :
          new String[] { Type.getInternalName(PermazenObject.class) };
        cw.visit(Opcodes.V1_6, Opcodes.ACC_PUBLIC | Opcodes.ACC_SUPER | Opcodes.ACC_SYNTHETIC,
          this.getClassName(), null, this.getSuperclassName(), interfaces);
        cw.visitSource(GEN_SOURCE, null);
        this.outputFields(cw);
        this.outputConstructors(cw);
        this.outputMethods(cw);
        cw.visitEnd();
        final byte[] classfile = cw.toByteArray();
        if (this.log.isTraceEnabled())
            this.log.trace("done generating class {}", this.getClassName());
        this.debugDump(System.out, classfile);

        // Done
        return classfile;
    }

    private void outputFields(ClassWriter cw) {

        // Output "$tx" field
        cw.visitField(Opcodes.ACC_PROTECTED | Opcodes.ACC_FINAL | Opcodes.ACC_TRANSIENT,
          TX_FIELD_NAME, Type.getDescriptor(PermazenTransaction.class), null, null).visitEnd();

        // Output "$id" field
        cw.visitField(Opcodes.ACC_PROTECTED | Opcodes.ACC_FINAL,
          ID_FIELD_NAME, Type.getDescriptor(ObjId.class), null, null).visitEnd();

        // Output fields associated with PermazenFields
        if (this.pclass != null) {
            for (PermazenField pfield : this.pclass.fieldsByName.values())
                pfield.outputFields(this, cw);
        }

        // Output field(s) for cached simple field flags
        if (this.pclass != null) {
            String lastFieldName = null;
            for (int i = 0; i < this.simpleFieldNames.size(); i++) {
                final String fieldName = this.getCachedFlagFieldName(i);
                if (fieldName.equals(lastFieldName))
                    continue;
                final FieldVisitor flagsField = cw.visitField(Opcodes.ACC_PRIVATE | Opcodes.ACC_TRANSIENT,
                  fieldName, Type.getDescriptor(this.getCachedFlagEncoding(i)), null, null);
                flagsField.visitEnd();
                lastFieldName = fieldName;
            }
        }

        // Output (static) @ReferencePath cached ReferencePath fields
        if (this.pclass != null) {
            int fieldIndex = 0;
            for (ReferencePathScanner<?>.ReferencePathMethodInfo info : this.pclass.referencePathMethods) {
                final String fieldName = REFERENCE_PATH_FIELD_PREFIX + fieldIndex++;
                cw.visitField(Opcodes.ACC_PRIVATE | Opcodes.ACC_STATIC,
                  fieldName, Type.getDescriptor(ReferencePath.class), null, null).visitEnd();
            }
        }
    }

    private void outputConstructors(ClassWriter cw) {

        // Foo(PermazenTransaction tx, ObjId id)
        MethodVisitor mv = cw.visitMethod(Opcodes.ACC_PUBLIC, "<init>",
          Type.getMethodDescriptor(Type.VOID_TYPE, Type.getType(PermazenTransaction.class), Type.getType(ObjId.class)), null, null);
        mv.visitCode();
        mv.visitVarInsn(Opcodes.ALOAD, 0);
        mv.visitInsn(Opcodes.DUP);
        mv.visitInsn(Opcodes.DUP);
        mv.visitVarInsn(Opcodes.ALOAD, 1);                                                              // this.tx = tx
        mv.visitFieldInsn(Opcodes.PUTFIELD, this.getClassName(), TX_FIELD_NAME, Type.getDescriptor(PermazenTransaction.class));
        mv.visitVarInsn(Opcodes.ALOAD, 2);                                                              // this.id = id
        mv.visitFieldInsn(Opcodes.PUTFIELD, this.getClassName(), ID_FIELD_NAME, Type.getDescriptor(ObjId.class));
        if (this.superclassConstructor.getParameterCount() > 0) {
            mv.visitVarInsn(Opcodes.ALOAD, 1);
            mv.visitVarInsn(Opcodes.ALOAD, 2);
        }
        this.emitInvoke(mv, this.superclassConstructor);                                                // super(...)
        mv.visitInsn(Opcodes.RETURN);
        mv.visitMaxs(0, 0);
        mv.visitEnd();
    }

    private void outputMethods(ClassWriter cw) {

        // Output <clinit>, if needed
        if (this.pclass != null) {

            // Do any fields require initialization bytecode?
            boolean needClassInitializer = false;
            for (PermazenField pfield : this.pclass.fieldsByName.values()) {
                if (pfield.hasClassInitializerBytecode()) {
                    needClassInitializer = true;
                    break;
                }
            }

            // If so, add <clinit> method
            if (needClassInitializer) {
                MethodVisitor mv = cw.visitMethod(Opcodes.ACC_STATIC | Opcodes.ACC_PRIVATE, "<clinit>", "()V", null, null);
                mv.visitCode();
                for (PermazenField pfield : this.pclass.fieldsByName.values()) {
                    if (pfield.hasClassInitializerBytecode())
                        pfield.outputClassInitializerBytecode(this, mv);
                }
                mv.visitInsn(Opcodes.RETURN);
                mv.visitMaxs(0, 0);
                mv.visitEnd();
            }
        }

        // Output PermazenObject.getTransaction()
        MethodVisitor mv = this.startMethod(cw, PERMAZEN_OBJECT_GET_PERMAZEN_TRANSACTION_METHOD);
        mv.visitCode();
        mv.visitVarInsn(Opcodes.ALOAD, 0);
        mv.visitFieldInsn(Opcodes.GETFIELD, this.getClassName(), TX_FIELD_NAME, Type.getDescriptor(PermazenTransaction.class));
        mv.visitInsn(Opcodes.ARETURN);
        mv.visitMaxs(0, 0);
        mv.visitEnd();

        // Output PermazenObject.getModelClass()
        mv = this.startMethod(cw, PERMAZEN_OBJECT_GET_MODEL_CLASS_METHOD);
        mv.visitCode();
        mv.visitLdcInsn(Type.getObjectType(Type.getInternalName(this.modelClass)));
        mv.visitInsn(Opcodes.ARETURN);
        mv.visitMaxs(0, 0);
        mv.visitEnd();

        // Add PermazenObject.getObjId()
        mv = this.startMethod(cw, PERMAZEN_OBJECT_GET_OBJ_ID_METHOD);
        mv.visitCode();
        mv.visitVarInsn(Opcodes.ALOAD, 0);
        mv.visitFieldInsn(Opcodes.GETFIELD, this.getClassName(), ID_FIELD_NAME, Type.getDescriptor(ObjId.class));
        mv.visitInsn(Opcodes.ARETURN);
        mv.visitMaxs(0, 0);
        mv.visitEnd();

        // Add PermazenObject.resetCachedFieldValues()
        mv = this.startMethod(cw, PERMAZEN_OBJECT_RESET_CACHED_FIELD_VALUES_METHOD);
        mv.visitCode();
        if (this.pclass != null) {
            String lastFieldName = null;
            for (int i = 0; i < this.simpleFieldNames.size(); i++) {
                final String fieldName = this.getCachedFlagFieldName(i);
                if (fieldName.equals(lastFieldName))
                    continue;
                mv.visitVarInsn(Opcodes.ALOAD, 0);
                mv.visitInsn(Opcodes.ICONST_0);
                mv.visitFieldInsn(Opcodes.PUTFIELD, this.getClassName(),
                  fieldName, Type.getDescriptor(this.getCachedFlagEncoding(i)));
                lastFieldName = fieldName;
            }
        }
        mv.visitInsn(Opcodes.RETURN);
        mv.visitMaxs(0, 0);
        mv.visitEnd();

        // Add PermazenObject.toString() - if not already overridden
        final Method modelClassToString;
        if (this.modelClass.isInterface())
            modelClassToString = OBJECT_TO_STRING_METHOD;
        else {
            try {
                modelClassToString = this.modelClass.getMethod("toString");
            } catch (NoSuchMethodException e) {
                throw new RuntimeException("unexpected exception", e);
            }
        }
        if (modelClassToString.equals(OBJECT_TO_STRING_METHOD)) {
            mv = this.startMethod(cw, OBJECT_TO_STRING_METHOD);
            mv.visitCode();
            mv.visitVarInsn(Opcodes.ALOAD, 0);
            this.emitInvoke(mv, PERMAZEN_OBJECT_GET_PERMAZEN_TRANSACTION_METHOD);
            this.emitInvoke(mv, PERMAZEN_TRANSACTION_GET_TRANSACTION_METHOD);
            mv.visitVarInsn(Opcodes.ALOAD, 0);
            mv.visitFieldInsn(Opcodes.GETFIELD, this.getClassName(), ID_FIELD_NAME, Type.getDescriptor(ObjId.class));
            mv.visitLdcInsn(TO_STRING_MAX_COLLECTION_ENTRIES);
            this.emitInvoke(mv, OBJ_DUMPER_TO_STRING_METHOD);
            mv.visitInsn(Opcodes.ARETURN);
            mv.visitMaxs(0, 0);
            mv.visitEnd();
        }

        // If no associated PermazenClass, we're done
        if (this.pclass == null)
            return;

        // Add methods that override field getters & setters
        for (PermazenField pfield : this.pclass.fieldsByName.values())
            pfield.outputMethods(this, cw);

        // Add @ReferencePath methods
        int fieldIndex = 0;
        for (ReferencePathScanner<?>.ReferencePathMethodInfo info : this.pclass.referencePathMethods)
            this.addReferencePathMethod(cw, info, REFERENCE_PATH_FIELD_PREFIX + fieldIndex++);
    }

    private void addReferencePathMethod(ClassWriter cw, ReferencePathScanner<?>.ReferencePathMethodInfo info, String fieldName) {
        final MethodVisitor mv = this.startMethod(cw, info.getMethod());

        // Check if we have cached the path already
        mv.visitFieldInsn(Opcodes.GETSTATIC, this.getClassName(), fieldName, Type.getDescriptor(ReferencePath.class));
        final Label gotPath = new Label();
        mv.visitJumpInsn(Opcodes.IFNONNULL, gotPath);

        // Parse the path and cache it in this instance
        final ReferencePath path = info.getReferencePath();
        mv.visitVarInsn(Opcodes.ALOAD, 0);
        mv.visitFieldInsn(Opcodes.GETFIELD, this.getClassName(), TX_FIELD_NAME, Type.getDescriptor(PermazenTransaction.class));
        this.emitInvoke(mv, ClassGenerator.PERMAZEN_TRANSACTION_GET_PERMAZEN_METHOD);
        mv.visitVarInsn(Opcodes.ALOAD, 0);
        this.emitInvoke(mv, ClassGenerator.OBJECT_GET_CLASS_METHOD);
        this.emitInvoke(mv, ClassGenerator.CLASS_GET_SUPERCLASS_METHOD);
        mv.visitLdcInsn(path.toString());
        this.emitInvoke(mv, ClassGenerator.PERMAZEN_PARSE_REFERENCE_PATH_METHOD);
        mv.visitFieldInsn(Opcodes.PUTSTATIC, this.getClassName(), fieldName, Type.getDescriptor(ReferencePath.class));

        // Traverse the path
        mv.visitLabel(gotPath);
        mv.visitFrame(Opcodes.F_SAME, 0, new Object[0], 0, new Object[0]);
        mv.visitVarInsn(Opcodes.ALOAD, 0);
        mv.visitFieldInsn(Opcodes.GETFIELD, this.getClassName(), TX_FIELD_NAME, Type.getDescriptor(PermazenTransaction.class));
        mv.visitFieldInsn(Opcodes.GETSTATIC, this.getClassName(), fieldName, Type.getDescriptor(ReferencePath.class));
        mv.visitVarInsn(Opcodes.ALOAD, 0);
        this.emitInvoke(mv, ClassGenerator.UTIL_STREAM_OF_METHOD);
        this.emitInvoke(mv, PERMAZEN_TRANSACTION_FOLLOW_REFERENCE_PATH_METHOD);

        // Extract first element if method returns Optional
        if (info.isReturnsOptional()) {
            final Label tryStart = new Label();
            final Label tryStop = new Label();
            final Label catchLabel = new Label();
            mv.visitTryCatchBlock(tryStart, tryStop, catchLabel, Type.getInternalName(NoSuchElementException.class));
            mv.visitLabel(tryStart);
            this.emitInvoke(mv, ClassGenerator.SORTED_SET_FIRST_METHOD);
            mv.visitLabel(tryStop);
            this.emitInvoke(mv, ClassGenerator.OPTIONAL_OF_METHOD);
            mv.visitInsn(Opcodes.ARETURN);
            mv.visitLabel(catchLabel);
            this.emitInvoke(mv, ClassGenerator.OPTIONAL_EMPTY_METHOD);
            mv.visitInsn(Opcodes.ARETURN);
        }

        // Return result
        mv.visitInsn(Opcodes.ARETURN);

        // Done
        mv.visitMaxs(0, 0);
        mv.visitEnd();
    }

// Helper Methods

    // Debug dump - requires asm-util
    protected void debugDump(PrintStream out, byte[] classfile) {
        // CHECKSTYLE OFF: GenericIllegalRegexp
        // java.io.PrintWriter pw = new java.io.PrintWriter(out, true);
        // pw.println("***************** BEGIN CLASSFILE ******************");
        // org.objectweb.asm.ClassReader cr = new org.objectweb.asm.ClassReader(classfile);
        // cr.accept(new org.objectweb.asm.util.TraceClassVisitor(pw), 0);
        // pw.flush();
        // pw.println("***************** END CLASSFILE ******************");
        // CHECKSTYLE ON: GenericIllegalRegexp
    }

    /**
     * Emit code that wraps the primitive value on the top of the stack.
     */
    void wrap(MethodVisitor mv, Primitive<?> primitive) {
        final Type wrapperType = Type.getType(primitive.getWrapperType());
        mv.visitMethodInsn(Opcodes.INVOKESTATIC, wrapperType.getInternalName(), "valueOf",
          Type.getMethodDescriptor(wrapperType, Type.getType(primitive.getType())), false);
    }

    /**
     * Emit code that unwraps the primitive value on the top of the stack.
     */
    void unwrap(MethodVisitor mv, Primitive<?> primitive) {
        final Method unwrapMethod = primitive.getUnwrapMethod();
        this.emitInvoke(mv, unwrapMethod);
    }

    /**
     * Emit code to invoke a method. This assumes the stack is loaded.
     */
    void emitInvoke(MethodVisitor mv, Method method) {
        final boolean isInterface = method.getDeclaringClass().isInterface();
        final boolean isStatic = (method.getModifiers() & Modifier.STATIC) != 0;
        mv.visitMethodInsn(isStatic ? Opcodes.INVOKESTATIC : isInterface ? Opcodes.INVOKEINTERFACE : Opcodes.INVOKEVIRTUAL,
          Type.getInternalName(method.getDeclaringClass()), method.getName(), Type.getMethodDescriptor(method), isInterface);
    }

    /**
     * Emit code to INVOKEVIRTUAL a method using the specified class. This assumes the stack is loaded.
     */
    void emitInvoke(MethodVisitor mv, String className, Method method) {
        mv.visitMethodInsn(Opcodes.INVOKEVIRTUAL, className, method.getName(), Type.getMethodDescriptor(method), false);
    }

    /**
     * Emit code to INVOKESPECIAL a constructor using the specified class. This assumes the stack is loaded.
     */
    void emitInvoke(MethodVisitor mv, Constructor<?> constructor) {
        mv.visitMethodInsn(Opcodes.INVOKESPECIAL, Type.getInternalName(constructor.getDeclaringClass()),
          "<init>", Type.getConstructorDescriptor(constructor), false);
    }

    /**
     * Create {@link MethodVisitor} to implement or override the given method.
     */
    MethodVisitor startMethod(ClassWriter cw, Method method) {
        return cw.visitMethod(
          method.getModifiers() & (Opcodes.ACC_PUBLIC | Opcodes.ACC_PRIVATE | Opcodes.ACC_PROTECTED),
          method.getName(), Type.getMethodDescriptor(method), null, this.getExceptionNames(method));
    }

    /**
     * Get list of exception types for method.
     */
    String[] getExceptionNames(Method method) {
        ArrayList<String> list = new ArrayList<>();
        for (Class<?> type : method.getExceptionTypes())
            list.add(Type.getType(type).getInternalName());
        return list.toArray(new String[list.size()]);
    }

// Cached value flags field(s)

    String getCachedFlagFieldName(PermazenSimpleField pfield) {
        return this.getCachedFlagFieldName(this.getCachedFlagIndex(pfield));
    }

    int getCachedFlagBit(PermazenSimpleField pfield) {
        return 1 << (this.getCachedFlagIndex(pfield) % 32);
    }

    Class<?> getCachedFlagEncoding(PermazenSimpleField pfield) {
        return this.getCachedFlagEncoding(this.getCachedFlagIndex(pfield));
    }

    private Class<?> getCachedFlagEncoding(int simpleFieldIndex) {
        final int numSimpleFields = this.simpleFieldNames.size();
        Preconditions.checkArgument(simpleFieldIndex >= 0 && simpleFieldIndex < numSimpleFields);
        if (simpleFieldIndex / 32 < numSimpleFields / 32)
            return int.class;
        final int tail = numSimpleFields % 32;
        return tail <= 8 ? byte.class : tail <= 16 ? short.class : int.class;
    }

    private String getCachedFlagFieldName(int simpleFieldIndex) {
        Preconditions.checkArgument(simpleFieldIndex >= 0 && simpleFieldIndex < this.simpleFieldNames.size());
        return ClassGenerator.CACHED_FLAG_FIELD_PREFIX + (simpleFieldIndex / 32);
    }

    private int getCachedFlagIndex(PermazenSimpleField pfield) {
        Preconditions.checkArgument(pfield.parent == this.pclass);
        final int simpleFieldIndex = this.simpleFieldNames.indexOf(pfield.name);
        Preconditions.checkArgument(simpleFieldIndex != -1);
        return simpleFieldIndex;
    }
}
