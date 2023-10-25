
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.core;

import com.google.common.base.Preconditions;

import java.io.Serializable;

/**
 * A globally unique identifier for a simple field encoding scheme.
 *
 * <p>
 * Encoding ID's are Uniform Resource Names (URNs), with these differences:
 * <ul>
 *  <li>The first two fields (the {@code "urn"} and the {@code NID}) must be lowercase.
 *  <li>The optional r-components, q-components, and f-components are disallowed.
 *  <li>There may be from 1 to 255 trailing {@code []} pairs, indicating an array type.
 * </ul>
 *
 * <p>
 * Valid encoding ID's must match {@link #PATTERN}, which encodes the above rules.
 *
 * @see FieldType
 * @see <a href="https://datatracker.ietf.org/doc/html/rfc8141">RFC 8141</a>
 */
public class EncodingId implements Comparable<EncodingId>, Serializable {

    /**
     * Encoding ID array suffix.
     */
    public static final String ARRAY_SUFFIX = "[]";

    private static final long serialVersionUID = 5736000512433844834L;

    private static final String ALPHANUM = "[a-z0-9]";
    private static final String UNRESERVED = "[-._~A-Za-z0-9]";
    private static final String PCT_ENCODED = "(%[A-fa-f0-9]{2})";
    private static final String SUB_DELIMS = "[!$&'()*+,;=]";
    private static final String LDH = "[-a-z0-9]";
    private static final String NID = "(" + ALPHANUM + "(" + LDH + "{0,30})" + ALPHANUM + ")";
    private static final String PCHAR = "(" + UNRESERVED + "|" + PCT_ENCODED + "|" + SUB_DELIMS + "|[:@])";
    private static final String NSS = "(" + PCHAR + "(" + PCHAR + "|/)*)";
    private static final String SIMPLE_URN = "(urn:" + NID + ":" + NSS + ")";

    /**
     * The regular expression that encoding ID's must match.
     */
    public static final String PATTERN = SIMPLE_URN + "(\\[\\]){0,255}";

    private final String id;

// Constructors

    /**
     * Constructor.
     *
     * @param id the encoding ID
     * @throws IllegalArgumentException if {@code id} is null or invalid
     */
    public EncodingId(String id) {
        Preconditions.checkArgument(id != null, "null id");
        Preconditions.checkArgument(id.matches(PATTERN), "invalid encoding ID \"" + id + "\"");
        this.id = id;
    }

// Public methods

    /**
     * Get the encoding ID.
     *
     * @return encoding ID
     */
    public String getId() {
        return this.id;
    }

    /**
     * Get the base encoding ID.
     *
     * <p>
     * If this ID does not specify an array encoding, this is just the ID itself.
     * Otherwise, this method returns the ID without any trailing {@code []} pairs.
     *
     * @return this encoding ID without any array dimensions
     */
    public String getBaseId() {
        final int index = this.id.indexOf('[');
        return index == -1 ? this.id : this.id.substring(0, index);
    }

    /**
     * Get the number of array dimensions.
     *
     * @return number of array dimensions, from zero to 255
     */
    public int getArrayDimensions() {
        final int index = this.id.indexOf('[');
        return index == -1 ? 0 : (this.id.length() - index) / 2;
    }

    /**
     * Get the encoding ID corresponding to one fewer array dimension.
     *
     * @return element encoding ID
     * @throws IllegalArgumentException if this instance has zero array dimensions
     */
    public EncodingId getElementId() {
        final int index = this.id.indexOf('[');
        Preconditions.checkArgument(index > 0, "zero array dimensions");
        return new EncodingId(this.id.substring(0, this.id.length() - 2));
    }

    /**
     * Get the encoding ID corresponding to one more array dimension.
     *
     * @return element encoding ID
     * @throws IllegalArgumentException if this instance already has the maximum number of array dimensions
     */
    public EncodingId getArrayId() {
        Preconditions.checkArgument(this.getArrayDimensions() < FieldType.MAX_ARRAY_DIMENSIONS, "too many array dimensions");
        return new EncodingId(this.id + ARRAY_SUFFIX);
    }

// Comparable

    @Override
    public int compareTo(EncodingId that) {
        return this.id.compareTo(that.id);
    }

// Object

    @Override
    public String toString() {
        return this.id;
    }

    @Override
    public int hashCode() {
        return this.getClass().hashCode() ^ this.id.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this)
            return true;
        if (obj == null || obj.getClass() != this.getClass())
            return false;
        final EncodingId that = (EncodingId)obj;
        return this.id.equals(that.id);
    }
}
