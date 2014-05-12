
/*
 * Copyright (C) 2011 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.dellroad.stuff.string;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Utility class supporting parsing of strings.
 *
 * <p>
 * Instances of this class are not thread safe.
 * </p>
 */
public class ParseContext implements Cloneable {

    private static final int MAX_REJECT_QUOTE = 15;

    private final String input;

    private int index;

    /**
     * Constructor.
     *
     * @param input the input string to parse
     * @throws IllegalArgumentException if {@code input} is null
     */
    public ParseContext(String input) {
        if (input == null)
            throw new IllegalArgumentException("null input");
        this.input = input;
    }

    /**
     * Get the original input string as passed to the constructor.
     *
     * @return original input string
     */
    public String getOriginalInput() {
        return this.input;
    }

    /**
     * Get the current input.
     *
     * @return substring of the original input string starting at the current parse position
     */
    public String getInput() {
        return this.input.substring(this.index);
    }

    /**
     * Get the current index into the original input string.
     *
     * @return current parse position
     * @see #setIndex
     */
    public int getIndex() {
        return this.index;
    }

    /**
     * Set the current index into the original input string.
     *
     * @param index new parse position
     * @throws IllegalArgumentException if {@code index} is greater than the original string length
     * @see #getIndex
     */
    public void setIndex(int index) {
        this.index = index;
    }

    /**
     * Reset this instance. This instance will return to the state it was in
     * immediately after construction.
     *
     * <p>
     * This method just invokes:
     * <blockquote>
     *  <code>setIndex(0)</code>
     * </blockquote>
     */
    public void reset() {
        this.setIndex(0);
    }

    /**
     * Match the current input against the given regular expression and advance past it.
     *
     * @param regex regular expression to match against the current input
     * @throws IllegalArgumentException if the current input does not match
     */
    public Matcher matchPrefix(String regex) {
        return this.matchPrefix(Pattern.compile(regex));
    }

    /**
     * Match the current input against the given regular expression and advance past it.
     *
     * @param regex regular expression to match against the current input
     * @throws IllegalArgumentException if the current input does not match
     */
    public Matcher matchPrefix(Pattern regex) {
        final Matcher matcher = this.tryPattern(regex);
        if (matcher == null)
            throw buildException("expected input matching pattern `" + regex + "'");
        return matcher;
    }

    /**
     * Determine if the current input starts with the given literal prefix.
     * If so, advance past it. If not, do not advance.
     *
     * @param prefix literal string to try to match against the current input
     * @return whether the current input matched {@code prefix}
     */
    public boolean tryLiteral(String prefix) {
        final boolean match = this.input.startsWith(prefix, this.index);
        if (match)
            this.index += prefix.length();
        return match;
    }

    /**
     * Determine if the current input starts with the given regular expression.
     * If so, advance past it and return a successful {@link Matcher}; otherwise, return null.
     *
     * @param pattern regular expression to match against input
     * @return match if successful, null if not
     */
    public Matcher tryPattern(String pattern) {
        return this.tryPattern(Pattern.compile(pattern));
    }

    /**
     * Determine if the current input starts with the given regular expression.
     * If so, advance past it and return a successful {@link Matcher}; otherwise, return null.
     *
     * @param regex regular expression to match against input
     * @return match if successful, null if not
     */
    public Matcher tryPattern(Pattern regex) {
        final Matcher matcher = regex.matcher(this.getInput());
        if (!matcher.lookingAt())
            return null;
        this.index += matcher.end();
        return matcher;
    }

    /**
     * Determine if we are at the end of the input.
     */
    public boolean isEOF() {
        return this.index >= this.input.length();
    }

    /**
     * Read and advance past the next character.
     *
     * @return the next character of input
     * @throws IllegalArgumentException if there are no more characters
     */
    public char read() {
        char ch = this.peek();
        this.index++;
        return ch;
    }

    /**
     * Read, but do not advance past, the next character.
     *
     * @return the next character of input
     * @throws IllegalArgumentException if there are no more characters
     */
    public char peek() {
        try {
            return this.input.charAt(this.index);
        } catch (StringIndexOutOfBoundsException e) {
            throw this.buildException("truncated input");
        }
    }

    /**
     * Push back the previously read character.
     *
     * @throws IllegalStateException if the beginning of the original string has been reached
     */
    public void unread() {
        if (this.index == 0)
            throw new IllegalStateException();
        this.index--;
    }

    /**
     * Read and advance past the next character, which must match {@code ch}.
     *
     * @param ch the expected next character of input
     * @throws IllegalArgumentException if there are no more characters or the
     *  next character read is not {@code ch}
     */
    public void expect(char ch) {
        if (this.read() != ch) {
            this.unread();
            throw buildException("expected `" + ch + "'");
        }
    }

    /**
     * Skip leading whitespace, if any.
     *
     * @see Character#isWhitespace
     */
    public void skipWhitespace() {
        while (this.index < this.input.length() && Character.isWhitespace(this.input.charAt(this.index)))
            this.index++;
    }

    /**
     * Clone this instance.
     */
    public ParseContext clone() {
        try {
            return (ParseContext)super.clone();
        } catch (CloneNotSupportedException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Create a generic exception for rejecting the current input.
     */
    public IllegalArgumentException buildException() {
        return this.buildException(null);
    }

    /**
     * Create an exception for rejecting the current input.
     *
     * @param message problem description, or {@code null} for none
     */
    public IllegalArgumentException buildException(String message) {
        String text = "parse error ";
        String bogus = getInput();
        if (bogus.length() == 0)
            text += "at end of input";
        else {
            if (bogus.length() > MAX_REJECT_QUOTE)
                bogus = bogus.substring(0, MAX_REJECT_QUOTE - 3) + "...";
            text += "starting with `" + bogus + "'";
        }
        if (message != null)
            text += ": " + message;
        return new IllegalArgumentException(text);
    }
}

