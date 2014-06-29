
/*
 * Copyright (C) 2011 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.dellroad.stuff.java;

import java.lang.reflect.Method;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;
import java.util.regex.Pattern;

/**
 * Simple utility enumeration for working Java {@link Class} instances representing primitive types (including {@code void}).
 *
 * <p>
 * Instances of this class represent the eight Java primitive types plus {@code void}.
 * <p/>
 *
 * @param <T> Primitive wrapper type
 */
public abstract class Primitive<T> implements Comparator<T> {

    /**
     * Void type. The {@link #getDefaultValue}, {@link #compare compare()}, and {@link #parseValue parseValue()}
     * methods always throw exceptions.
     */
    public static final Primitive<Void> VOID = new Primitive<Void>(Void.TYPE, Void.class, 'V', "") {
        @Override
        public <R> R visit(PrimitiveSwitch<R> pswitch) {
            return pswitch.caseVoid();
        }
        @Override
        public Void getDefaultValue() {
            throw new UnsupportedOperationException();
        }
        @Override
        public int compare(Void value1, Void value2) {
            throw new NullPointerException();
        }
        @Override
        protected Void doParseValue(String string) {
            throw new IllegalArgumentException();
        }
    };

    /**
     * Boolean type. String values must equal {@code "true"} or {@code "false"}.
     */
    public static final Primitive<Boolean> BOOLEAN = new Primitive<Boolean>(Boolean.TYPE, Boolean.class, 'Z', "true|false") {
        @Override
        public <R> R visit(PrimitiveSwitch<R> pswitch) {
            return pswitch.caseBoolean();
        }
        @Override
        public Boolean getDefaultValue() {
            return false;
        }
        @Override
        public int compare(Boolean value1, Boolean value2) {
            return Boolean.compare(value1, value2);
        }
        @Override
        protected Boolean doParseValue(String string) {
            return Boolean.parseBoolean(string);
        }
    };

    /**
     * Byte type. String values are parsed using {@link Byte#decode Byte.decode()}.
     */
    public static final Primitive<Byte> BYTE = new Primitive<Byte>(Byte.TYPE, Byte.class, 'B',
      "(?i)(\\+|-)?(((0x|#)0*([A-F0-9]{1,2}))|(0+[0-7]{0,3})|([1-9][0-9]{0,2}))") {
        @Override
        public <R> R visit(PrimitiveSwitch<R> pswitch) {
            return pswitch.caseByte();
        }
        @Override
        public Byte getDefaultValue() {
            return (byte)0;
        }
        @Override
        public int compare(Byte value1, Byte value2) {
            return Byte.compare(value1, value2);
        }
        @Override
        protected Byte doParseValue(String string) {
            return Byte.decode(string);
        }
    };

    /**
     * Character type. String values must be exactly one character long.
     */
    public static final Primitive<Character> CHARACTER = new Primitive<Character>(Character.TYPE, Character.class, 'C', "(?s).") {
        @Override
        public <R> R visit(PrimitiveSwitch<R> pswitch) {
            return pswitch.caseCharacter();
        }
        @Override
        public Character getDefaultValue() {
            return (char)0;
        }
        @Override
        public int compare(Character value1, Character value2) {
            return Character.compare(value1, value2);
        }
        @Override
        protected Character doParseValue(String string) {
            if (string.length() != 1)
                throw new IllegalArgumentException("string has length " + string.length() + " != 1");
            return Character.valueOf(string.charAt(0));
        }
    };

    /**
     * Short type. String values are parsed using {@link Short#decode Short.decode()}.
     */
    public static final Primitive<Short> SHORT = new Primitive<Short>(Short.TYPE, Short.class, 'S',
      "(?i)(\\+|-)?(((0x|#)0*([A-F0-9]{1,4}))|(0+[0-7]{0,6})|([1-9][0-9]{0,4}))") {
        @Override
        public <R> R visit(PrimitiveSwitch<R> pswitch) {
            return pswitch.caseShort();
        }
        @Override
        public Short getDefaultValue() {
            return (short)0;
        }
        @Override
        public int compare(Short value1, Short value2) {
            return Short.compare(value1, value2);
        }
        @Override
        protected Short doParseValue(String string) {
            return Short.decode(string);
        }
    };

    /**
     * Integer type. String values are parsed using {@link Integer#decode Integer.decode()}.
     */
    public static final Primitive<Integer> INTEGER = new Primitive<Integer>(Integer.TYPE, Integer.class, 'I',
      "(?i)(\\+|-)?(((0x|#)0*([A-F0-9]{1,8}))|(0+[0-7]{0,11})|([1-9][0-9]{0,9}))") {
        @Override
        public <R> R visit(PrimitiveSwitch<R> pswitch) {
            return pswitch.caseInteger();
        }
        @Override
        public Integer getDefaultValue() {
            return 0;
        }
        @Override
        public int compare(Integer value1, Integer value2) {
            return Integer.compare(value1, value2);
        }
        @Override
        protected Integer doParseValue(String string) {
            return Integer.decode(string);
        }
    };

    /**
     * Float type. String values are parsed using {@link Float#parseFloat Float.parseFloat()}.
     */
    public static final Primitive<Float> FLOAT = new Primitive<Float>(Float.TYPE, Float.class, 'F', DoubleFormat.REGEX) {
        @Override
        public <R> R visit(PrimitiveSwitch<R> pswitch) {
            return pswitch.caseFloat();
        }
        @Override
        public Float getDefaultValue() {
            return 0.0f;
        }
        @Override
        public int compare(Float value1, Float value2) {
            return Float.compare(value1, value2);
        }
        @Override
        protected Float doParseValue(String string) {
            return Float.parseFloat(string);
        }
    };

    /**
     * Long type. String values are parsed using {@link Long#decode Long.decode()}.
     */
    public static final Primitive<Long> LONG = new Primitive<Long>(Long.TYPE, Long.class, 'J',
      "(?i)(\\+|-)?(((0x|#)0*([A-F0-9]{1,16}))|(0+[0-7]{0,22})|([1-9][0-9]{0,18}))") {
        @Override
        public <R> R visit(PrimitiveSwitch<R> pswitch) {
            return pswitch.caseLong();
        }
        @Override
        public Long getDefaultValue() {
            return 0L;
        }
        @Override
        public int compare(Long value1, Long value2) {
            return Long.compare(value1, value2);
        }
        @Override
        protected Long doParseValue(String string) {
            return Long.decode(string);
        }
    };

    /**
     * Double type. String values are parsed using {@link Double#parseDouble Double.parseDouble()}.
     */
    public static final Primitive<Double> DOUBLE = new Primitive<Double>(Double.TYPE, Double.class, 'D', DoubleFormat.REGEX) {
        @Override
        public <R> R visit(PrimitiveSwitch<R> pswitch) {
            return pswitch.caseDouble();
        }
        @Override
        public Double getDefaultValue() {
            return 0.0;
        }
        @Override
        public int compare(Double value1, Double value2) {
            return Double.compare(value1, value2);
        }
        @Override
        protected Double doParseValue(String string) {
            return Double.parseDouble(string);
        }
    };

    private static HashMap<Class<?>, Primitive<?>> classMap;

    private final Class<T> primType;
    private final Class<T> wrapType;
    private final char letter;
    private final Pattern parsePattern;

    Primitive(Class<T> primType, Class<T> wrapType, char letter, String parsePattern) {
        this.primType = primType;
        this.wrapType = wrapType;
        this.letter = letter;
        this.parsePattern = parsePattern != null ? Pattern.compile(parsePattern) : null;
        if (Primitive.classMap == null)
            Primitive.classMap = new HashMap<Class<?>, Primitive<?>>();
        Primitive.classMap.put(primType, this);
        Primitive.classMap.put(wrapType, this);
    }

    public abstract <R> R visit(PrimitiveSwitch<R> pswitch);

    /**
     * Get this primitive's default value.
     */
    public abstract T getDefaultValue();

    /**
     * Get the short name for this primitive type, e.g., "int".
     */
    public String getName() {
        return this.primType.getName();
    }

    /**
     * Get the long name for this primitive type, e.g., "Integer".
     * Also the simple name of the wrapper type.
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
    public Class<T> getType() {
        return this.primType;
    }

    /**
     * Get the wrapper {@link Class} object for this primitive type, e.g., {@code Integer.class}.
     */
    public Class<T> getWrapperType() {
        return this.wrapType;
    }

    /**
     * Get a regular expression that matches all allowed inputs to {@link #parseValue parseValue()}.
     * The returned pattern may also accept some values that {@link #parseValue parseValue()} rejects,
     * such as {@code 32768} for a {@code short}.
     */
    public Pattern getParsePattern() {
        if (this.parsePattern == null)
            throw new UnsupportedOperationException();
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
     * Compare two primitive values.
     *
     * @throws NullPointerException if either value is null
     */
    @Override
    public abstract int compare(T value1, T value2);

    /**
     * Parse a string-encoded value of this instance.
     * This method is guaranteed to accept all possible return values from the primitive's {@link #toString},
     * and possibly other reasonable inputs (e.g., hex and octal values for integral types).
     *
     * @param string string representation of a value
     * @throws IllegalArgumentException if {@code string} is null
     * @throws IllegalArgumentException if {@code string} does not match the {@linkplain #getParsePattern parse pattern}
     */
    public T parseValue(String string) {
        if (string == null)
            throw new IllegalArgumentException("null string");
        return this.doParseValue(string);
    }

    abstract T doParseValue(String string);

    /**
     * Get all instances of this class.
     *
     * @return a modifiable set containing all instances; modifications have no effect on this class
     */
    public static Set<Primitive<?>> values() {
        return new HashSet<Primitive<?>>(Primitive.classMap.values());
    }

    /**
     * Get the value corresponding to the given Java primitive or primitive wrapper type.
     *
     * @return the {@link Primitive} corresponding to {@code c}, or {@code null}
     *         if {@code c} is not a primitive or primitive wrapper type
     */
    public static Primitive<?> get(Class<?> c) {
        return Primitive.classMap.get(c);
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
             + ")[pP][+-]?" + DIGITS + "))" + "))";

        private DoubleFormat() {
        }
    }

    @Override
    public String toString() {
        return this.wrapType.getSimpleName().toUpperCase();
    }
}

