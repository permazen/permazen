
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb.util;

import java.util.ArrayList;
import java.util.TreeMap;
import java.util.TreeSet;

/**
 * Used to estimate Java object memory usage in bytes.
 *
 * <p>
 * For classes that contain references to other objects, what exactly is included in an object's "size"
 * is defined on a case-by-case basis.
 */
public class SizeEstimator {

    public static final int BOOLEAN_SIZE = 1;
    public static final int BYTE_SIZE = 1;
    public static final int CHAR_SIZE = 2;
    public static final int SHORT_SIZE = 2;
    public static final int INT_SIZE = 4;
    public static final int FLOAT_SIZE = 4;
    public static final int LONG_SIZE = 8;
    public static final int DOUBLE_SIZE = 8;

    private long total;

    /**
     * Get the estimated total size.
     *
     * @return current size estimate
     */
    public long getTotal() {
        return this.total;
    }

// Basic stuff

    /**
     * Get the basic overhead for any object.
     *
     * <p>
     * The implementation in {@code SizeEstimator} returns 12.
     *
     * @return minimum size of any {@link Object}
     */
    public int getObjectOverhead() {
        return 12;
    }

    /**
     * Get the size of an object reference.
     *
     * <p>
     * The implementation in {@code SizeEstimator} returns 4.
     *
     * @return the size of an object reference
     */
    public int getReferenceSize() {
        return 4;
    }

    /**
     * Add the basic overhead for any object.
     *
     * <p>
     * The implementation in {@code SizeEstimator} adds the amount returned by {@link #getObjectOverhead}.
     *
     * @return this instance
     */
    public SizeEstimator addObjectOverhead() {
        this.total += this.getObjectOverhead();
        return this;
    }

    /**
     * Adjust the total by an arbitrary amount.
     *
     * @param value amount to add to the total
     * @return this instance
     */
    public SizeEstimator add(long value) {
        this.total += value;
        return this;
    }

// Primitives

    /**
     * Add the size of a primitive {@code boolean} field.
     *
     * @return this instance
     */
    public final SizeEstimator addBooleanField() {
        this.total += BOOLEAN_SIZE;
        return this;
    }

    /**
     * Add the size of a primitive {@code byte} field.
     *
     * @return this instance
     */
    public final SizeEstimator addByteField() {
        this.total += BYTE_SIZE;
        return this;
    }

    /**
     * Add the size of a primitive {@code char} field.
     *
     * @return this instance
     */
    public final SizeEstimator addCharField() {
        this.total += CHAR_SIZE;
        return this;
    }

    /**
     * Add the size of a primitive {@code short} field.
     *
     * @return this instance
     */
    public final SizeEstimator addShortField() {
        this.total += SHORT_SIZE;
        return this;
    }

    /**
     * Add the size of a primitive {@code int} field.
     *
     * @return this instance
     */
    public final SizeEstimator addIntField() {
        this.total += INT_SIZE;
        return this;
    }

    /**
     * Add the size of a primitive {@code float} field.
     *
     * @return this instance
     */
    public final SizeEstimator addFloatField() {
        this.total += FLOAT_SIZE;
        return this;
    }

    /**
     * Add the size of a primitive {@code long} field.
     *
     * @return this instance
     */
    public final SizeEstimator addLongField() {
        this.total += LONG_SIZE;
        return this;
    }

    /**
     * Add the size of a primitive {@code double} field.
     *
     * @return this instance
     */
    public final SizeEstimator addDoubleField() {
        this.total += DOUBLE_SIZE;
        return this;
    }

    /**
     * Add the size of an object reference field.
     *
     * <p>
     * This is for the field itself, not including any object it may reference if not null
     *
     * @return this instance
     */
    public final SizeEstimator addReferenceField() {
        this.total += this.getReferenceSize();
        return this;
    }

// Arrays

    /**
     * Add the size of a non-null array having elements with the given size and the specified length.
     *
     * @param elementSize size of one array element
     * @param length array length
     * @return this instance
     * @throws IllegalArgumentException if {@code elementSize} is non-positive or {@code length} is negative
     */
    public SizeEstimator addArray(int elementSize, int length) {
        if (elementSize <= 0)
            throw new IllegalArgumentException("elementSize <= 0");
        if (length < 0)
            throw new IllegalArgumentException("length < 0");
        this.total +=
            this.getObjectOverhead()                                // for the array object
          + INT_SIZE                                                // for the array object's implicit length field
          + length * elementSize;                                   // for the array elements
        return this;
    }

    /**
     * Add the size of the given array object.
     *
     * @param array array object
     * @return this instance
     * @throws NullPointerException if {@code array} is null
     */
    public SizeEstimator add(boolean[] array) {
        return this.addArray(BOOLEAN_SIZE, array.length);
    }

    /**
     * Add the size of the given array object.
     *
     * @param array array object
     * @return this instance
     * @throws NullPointerException if {@code array} is null
     */
    public SizeEstimator add(byte[] array) {
        return this.addArray(BYTE_SIZE, array.length);
    }

    /**
     * Add the size of the given array object.
     *
     * @param array array object
     * @return this instance
     * @throws NullPointerException if {@code array} is null
     */
    public SizeEstimator add(char[] array) {
        return this.addArray(CHAR_SIZE, array.length);
    }

    /**
     * Add the size of the given array object.
     *
     * @param array array object
     * @return this instance
     * @throws NullPointerException if {@code array} is null
     */
    public SizeEstimator add(short[] array) {
        return this.addArray(SHORT_SIZE, array.length);
    }

    /**
     * Add the size of the given array object.
     *
     * @param array array object
     * @return this instance
     * @throws NullPointerException if {@code array} is null
     */
    public SizeEstimator add(int[] array) {
        return this.addArray(INT_SIZE, array.length);
    }

    /**
     * Add the size of the given array object.
     *
     * @param array array object
     * @return this instance
     * @throws NullPointerException if {@code array} is null
     */
    public SizeEstimator add(float[] array) {
        return this.addArray(FLOAT_SIZE, array.length);
    }

    /**
     * Add the size of the given array object.
     *
     * @param array array object
     * @return this instance
     * @throws NullPointerException if {@code array} is null
     */
    public SizeEstimator add(long[] array) {
        return this.addArray(LONG_SIZE, array.length);
    }

    /**
     * Add the size of the given array object.
     *
     * @param array array object
     * @return this instance
     * @throws NullPointerException if {@code array} is null
     */
    public SizeEstimator add(double[] array) {
        return this.addArray(DOUBLE_SIZE, array.length);
    }

    /**
     * Add the size of the given array object.
     *
     * <p>
     * This does not include the size of any objects referred to by elements in the array.
     *
     * @param array array object
     * @return this instance
     * @throws NullPointerException if {@code array} is null
     */
    public SizeEstimator add(Object[] array) {
        return this.addArray(this.getReferenceSize(), array.length);
    }

    /**
     * Add the size of the given array field.
     *
     * <p>
     * This includes the size of the array field itself, plus the size of the referred-to array if not null.
     *
     * @param array array object, possibly null
     * @return this instance
     */
    public SizeEstimator addField(boolean[] array) {
        if (array != null)
            this.add(array);
        return this.addReferenceField();
    }

    /**
     * Add the size of the given array field.
     *
     * <p>
     * This includes the size of the array field itself, plus the size of the referred-to array if not null.
     *
     * @param array array object, possibly null
     * @return this instance
     */
    public SizeEstimator addField(byte[] array) {
        if (array != null)
            this.add(array);
        return this.addReferenceField();
    }

    /**
     * Add the size of the given array field.
     *
     * <p>
     * This includes the size of the array field itself, plus the size of the referred-to array if not null.
     *
     * @param array array object, possibly null
     * @return this instance
     */
    public SizeEstimator addField(char[] array) {
        if (array != null)
            this.add(array);
        return this.addReferenceField();
    }

    /**
     * Add the size of the given array field.
     *
     * <p>
     * This includes the size of the array field itself, plus the size of the referred-to array if not null.
     *
     * @param array array object, possibly null
     * @return this instance
     */
    public SizeEstimator addField(short[] array) {
        if (array != null)
            this.add(array);
        return this.addReferenceField();
    }

    /**
     * Add the size of the given array field.
     *
     * <p>
     * This includes the size of the array field itself, plus the size of the referred-to array if not null.
     *
     * @param array array object, possibly null
     * @return this instance
     */
    public SizeEstimator addField(int[] array) {
        if (array != null)
            this.add(array);
        return this.addReferenceField();
    }

    /**
     * Add the size of the given array field.
     *
     * <p>
     * This includes the size of the array field itself, plus the size of the referred-to array if not null.
     *
     * @param array array object, possibly null
     * @return this instance
     */
    public SizeEstimator addField(float[] array) {
        if (array != null)
            this.add(array);
        return this.addReferenceField();
    }

    /**
     * Add the size of the given array field.
     *
     * <p>
     * This includes the size of the array field itself, plus the size of the referred-to array if not null.
     *
     * @param array array object, possibly null
     * @return this instance
     */
    public SizeEstimator addField(long[] array) {
        if (array != null)
            this.add(array);
        return this.addReferenceField();
    }

    /**
     * Add the size of the given array field.
     *
     * <p>
     * This includes the size of the array field itself, plus the size of the referred-to array if not null.
     *
     * @param array array object, possibly null
     * @return this instance
     */
    public SizeEstimator addField(double[] array) {
        if (array != null)
            this.add(array);
        return this.addReferenceField();
    }

    /**
     * Add the size of the given array field.
     *
     * <p>
     * This includes the size of the array field itself, plus the size of the referred-to array if not null.
     * This does not include the size of any objects referred to by elements in the array.
     *
     * @param array array object, possibly null
     * @return this instance
     */
    public SizeEstimator addField(Object[] array) {
        if (array != null)
            this.add(array);
        return this.addReferenceField();
    }

// SizeEstimating

    /**
     * Add the size of the given {@link SizeEstimating} object.
     *
     * <p>
     * This method just invokes {@link SizeEstimating#addTo SizeEstimating.addTo()}.
     *
     * @param obj object to measure
     * @return this instance
     * @throws NullPointerException if {@code obj} is null
     */
    public SizeEstimator add(SizeEstimating obj) {
        obj.addTo(this);
        return this;
    }

    /**
     * Add the size of a {@link SizeEstimating} reference field.
     *
     * <p>
     * This includes the size of the field itself, plus the size of the referred-to object
     * (according to {@link SizeEstimating#addTo SizeEstimating.addTo()} if {@code field} is not null.
     *
     * @param field field to measure, possibly null
     * @return this instance
     */
    public SizeEstimator addField(SizeEstimating field) {
        if (field != null)
            this.add(field);
        return this.addReferenceField();
    }

// Other

    /**
     * Add the size of an {@link ArrayList} field, assuming {@link ArrayList#trimToSize} has been invoked.
     *
     * <p>
     * This includes the size of the array field itself, plus the size of the referred-to array if {@code list} is not null.
     *
     * <p>
     * This does not include any objects referred to by the list.
     *
     * @param list {@link ArrayList} object, possibly null
     * @return this instance
     */
    public SizeEstimator addArrayListField(ArrayList<?> list) {
        if (list != null) {
            this.addObjectOverhead()                                    // ArrayList object
              .addReferenceField()                                      // elementData field
              .addArray(this.getReferenceSize(), list.size())           // elementData array
              .addIntField()                                            // modCount field
              .addIntField();                                           // size field
        }
        return this.addReferenceField();
    }

    /**
     * Add the size of a {@link TreeMap} field.
     *
     * <p>
     * This includes the size of the field itself, plus the size of the referred-to {@link TreeMap} if {@code map} is not null.
     *
     * <p>
     * This does not include the sizes of the key and value objects contained in the {@link TreeMap}.
     *
     * @param map {@link TreeMap} object, possibly null
     * @return this instance
     */
    public SizeEstimator addTreeMapField(TreeMap<?, ?> map) {
        if (map != null)
            this.total += this.getTreeMapSize(map.size());
        return this.addReferenceField();
    }

    /**
     * Add the size of a {@link TreeSet} field.
     *
     * <p>
     * This includes the size of the field itself, plus the size of the referred-to {@link TreeSet} if {@code set} is not null.
     *
     * <p>
     * This does not include the sizes of the objects contained in the {@link TreeSet}.
     *
     * @param set {@link TreeSet} object, possibly null
     * @return this instance
     */
    public SizeEstimator addTreeSetField(TreeSet<?> set) {
        if (set != null) {
            this.total +=
                this.getObjectOverhead()
              + this.getReferenceSize()                             // for the TreeSet.m field
              + this.getTreeMapSize(set.size());                    // for the TreeMap
        }
        return this.addReferenceField();
    }

    private long getTreeMapSize(int count) {

        // Get the size of one Entry object
        final int entrySize =
            this.getObjectOverhead()
          + this.getReferenceSize() * 5                             // parent, left, right, key, value
          + BOOLEAN_SIZE;                                           // color

        // Add the size of the TreeMap itself, plus all Entry's
        return this.getObjectOverhead()
          + this.getReferenceSize() * 7                             // various reference fields
          + INT_SIZE * 2                                            // size, modCount
          + entrySize * count;                                      // all Entry's
    }
}

