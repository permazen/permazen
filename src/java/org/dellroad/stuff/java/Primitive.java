
/*
 * Copyright (C) 2011 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.dellroad.stuff.java;

import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.regex.Pattern;

/**
 * Simple utility enumeration for working Java {@link Class} instances representing primitive types.
 *
 * <p>
 * Instances of this class represent one of the eight Java primitive types.
 * <p/>
 */
public enum Primitive {

    BOOLEAN(Boolean.TYPE, Boolean.class, 'Z', "true|false") {
        @Override
        public <R> R visit(PrimitiveSwitch<R> pswitch) {
            return pswitch.caseBoolean();
        }
        @Override
        public Object getDefaultValue() {
            return false;
        }
    },
    BYTE(Byte.TYPE, Byte.class, 'B', "(?i)(\\+|-)?(((0x|#)0*([A-F0-9]{1,2}))|(0+[0-7]{0,3})|([1-9][0-9]{0,2}))") {
        @Override
        public <R> R visit(PrimitiveSwitch<R> pswitch) {
            return pswitch.caseByte();
        }
        @Override
        public Object getDefaultValue() {
            return (byte)0;
        }
    },
    CHARACTER(Character.TYPE, Character.class, 'C', "(?s).") {
        @Override
        public <R> R visit(PrimitiveSwitch<R> pswitch) {
            return pswitch.caseCharacter();
        }
        @Override
        public Object getDefaultValue() {
            return (char)0;
        }
    },
    SHORT(Short.TYPE, Short.class, 'S', "(?i)(\\+|-)?(((0x|#)0*([A-F0-9]{1,4}))|(0+[0-7]{0,6})|([1-9][0-9]{0,4}))") {
        @Override
        public <R> R visit(PrimitiveSwitch<R> pswitch) {
            return pswitch.caseShort();
        }
        @Override
        public Object getDefaultValue() {
            return (short)0;
        }
    },
    INTEGER(Integer.TYPE, Integer.class, 'I', "(?i)(\\+|-)?(((0x|#)0*([A-F0-9]{1,8}))|(0+[0-7]{0,11})|([1-9][0-9]{0,9}))") {

        @Override
        public <R> R visit(PrimitiveSwitch<R> pswitch) {
            return pswitch.caseInteger();
        }
        @Override
        public Object getDefaultValue() {
            return 0;
        }
    },
    FLOAT(Float.TYPE, Float.class, 'F', DoubleFormat.REGEX) {
        @Override
        public <R> R visit(PrimitiveSwitch<R> pswitch) {
            return pswitch.caseFloat();
        }
        @Override
        public Object getDefaultValue() {
            return (float)0;
        }
    },
    LONG(Long.TYPE, Long.class, 'J', "(?i)(\\+|-)?(((0x|#)0*([A-F0-9]{1,16}))|(0+[0-7]{0,22})|([1-9][0-9]{0,18}))") {
        @Override
        public <R> R visit(PrimitiveSwitch<R> pswitch) {
            return pswitch.caseLong();
        }
        @Override
        public Object getDefaultValue() {
            return (long)0;
        }
    },
    DOUBLE(Double.TYPE, Double.class, 'D', DoubleFormat.REGEX) {
        @Override
        public <R> R visit(PrimitiveSwitch<R> pswitch) {
            return pswitch.caseDouble();
        }
        @Override
        public Object getDefaultValue() {
            return (double)0;
        }
    };

    private static final HashMap<Class<?>, Primitive> CLASS_MAP = new HashMap<Class<?>, Primitive>();

    static {
        for (Primitive prim : values()) {
            CLASS_MAP.put(prim.primType, prim);
            CLASS_MAP.put(prim.wrapType, prim);
        }
    }

    private final Class<?> primType;
    private final Class<?> wrapType;
    private final char letter;
    private final Pattern parsePattern;

    Primitive(Class<?> primType, Class<?> wrapType, char letter, String parsePattern) {
        this.primType = primType;
        this.wrapType = wrapType;
        this.letter = letter;
        this.parsePattern = Pattern.compile(parsePattern);
    }

    public abstract <R> R visit(PrimitiveSwitch<R> pswitch);

    public abstract Object getDefaultValue();

    /**
     * Get the short name for this primitive type, e.g., "int".
     */
    public String getName() {
        return this.primType.getName();
    }

    /**
     * Get the long name for this primitive type, e.g., "Integer".
     */
    public String getLongName() {
        return this.wrapType.getSimpleName();
    }

    /**
     * Get the single character descriptor for this primitive type, e.g., "I".
     */
    public char getLetter() {
        return this.letter;
    }

    /**
     * Get the {@link Class} object representing this primitive type, e.g., {@code Integer.TYPE}.
     */
    public Class<?> getType() {
        return this.primType;
    }

    /**
     * Get the wrapper {@link Class} object for this primitive type, e.g., {@code Integer.class}.
     */
    public Class<?> getWrapperType() {
        return this.wrapType;
    }

    /**
     * Get a regular expression that matches all allowed inputs to {@link #parseValue parseValue()}.
     * The returned pattern may also accept some values that {@link #parseValue parseValue()} rejects,
     * such as {@code 32768} for a {@code short}.
     */
    public Pattern getParsePattern() {
        return this.parsePattern;
    }

    /**
     * Get the wrapper class' "unwrap" method for this primitive type, e.g., {@code Integer.intValue()}.
     */
    public Method getUnwrapMethod() {
        try {
            return this.wrapType.getMethod(getName() + "Value");
        } catch (NoSuchMethodException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Parse a string-encoded value of this instance.
     * This method is guaranteed to accept all possible return values from the primitive's {@link #toString},
     * and possibly other reasonable inputs (e.g., hex and octal values for integral types).
     *
     * @param string string representation of a value
     * @throws IllegalArgumentException if {@code string} is null
     * @throws IllegalArgumentException if {@code string} does not match the {@linkplain #getParsePattern parse pattern}
     */
    public Object parseValue(String string) {

        // Handle null
        if (string == null)
            throw new IllegalArgumentException("null string");

        // Parse value
        switch (this) {
        case BOOLEAN:
            return Boolean.parseBoolean(string);
        case BYTE:
            return Byte.decode(string);
        case CHARACTER:
            if (string.length() != 1)
                throw new IllegalArgumentException("string has length " + string.length() + " != 1");
            return Character.valueOf(string.charAt(0));
        case SHORT:
            return Short.decode(string);
        case INTEGER:
            return Integer.decode(string);
        case FLOAT:
            return Float.parseFloat(string);
        case LONG:
            return Long.decode(string);
        case DOUBLE:
            return Double.parseDouble(string);
        default:
            throw new RuntimeException();
        }
    }

    /**
     * Get the value corresponding to the given Java primitive or primitive wrapper type.
     *
     * @return the {@link Primitive} corresponding to {@code c}, or {@code null}
     *         if {@code c} is not a primitive or primitive wrapper type
     */
    public static Primitive get(Class<?> c) {
        return CLASS_MAP.get(c);
    }

    // This is put into an inner class to avoid initialization ordering problems
    private static final class DoubleFormat {

        private static final String DIGITS = "(\\p{Digit}+)";

        private static final String HEX_DIGITS = "(\\p{XDigit}+)";

        // an exponent is 'e' or 'E' followed by an optionally
        // signed decimal integer.
        private static final String EXPONENT = "[eE][+-]?" + DIGITS;

        private static final String REGEX =
          "[+-]?("              // Optional sign character
          + "NaN|"              // "NaN" string
          + "Infinity|"         // "Infinity" string

          // A decimal floating-point string representing a finite positive
          // number without a leading sign has at most five basic pieces:
          // Digits . Digits ExponentPart FloatTypeSuffix
          //
          // Since this method allows integer-only strings as input
          // in addition to strings of floating-point literals, the
          // two sub-patterns below are simplifications of the grammar
          // productions from the Java Language Specification, 2nd
          // edition, section 3.10.2.

          // Digits ._opt Digits_opt ExponentPart_opt FloatTypeSuffix_opt
          + "(((" + DIGITS + "(\\.)?(" + DIGITS + "?)(" + EXPONENT + ")?)|"

          // . Digits ExponentPart_opt FloatTypeSuffix_opt
          + "(\\.(" + DIGITS + ")(" + EXPONENT + ")?)|"

          // Hexadecimal strings
          + "(("
            // 0[xX] HexDigits ._opt BinaryExponent FloatTypeSuffix_opt
            + "(0[xX]" + HEX_DIGITS + "(\\.)?)|"

            // 0[xX] HexDigits_opt . HexDigits BinaryExponent FloatTypeSuffix_opt
            + "(0[xX]" + HEX_DIGITS + "?(\\.)" + HEX_DIGITS + ")"
             + ")[pP][+-]?" + DIGITS + "))" + "[fFdD]?))";

        private DoubleFormat() {
        }
    }
}

