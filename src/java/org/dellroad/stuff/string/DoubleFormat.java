
/*
 * Copyright (C) 2011 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.dellroad.stuff.string;

import java.util.regex.Pattern;

/**
 * Java double string format regular expression. Adapted from the {@link Double.valueOf(String)} Javadoc.
 */
public final class DoubleFormat {

    /**
     * Regular expression which matches strings which are valid as input to {@link Double#valueOf(String)}.
     */
    public static final Pattern PATTERN;

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

    static {
        PATTERN = Pattern.compile(REGEX);
    }

    private DoubleFormat() {
    }
}

