
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen;

import io.permazen.core.ListField;
import io.permazen.core.MapField;
import io.permazen.core.SetField;

import java.lang.reflect.Method;
import java.nio.charset.Charset;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

/**
 * Default {@link StorageIdGenerator} implementation.
 *
 * <p>
 * This class hashes the names into storage ID's in the range {@value #MIN_STORAGE_ID} (inclusive) to
 * {@value #MAX_STORAGE_ID} (exclusive); this corresponds to the range of values that are encoded in three bytes.
 * This provides a target space of 65,280 possible storage IDs, so collisions should be extremely rare.
 */
public class DefaultStorageIdGenerator implements StorageIdGenerator {

    public static final int MIN_STORAGE_ID = 0x1fb;             // first value requiring three bytes
    public static final int MAX_STORAGE_ID = 0x100fb;           // first value requiring four bytes

    private final MessageDigest sha1;

    public DefaultStorageIdGenerator() {
        try {
            this.sha1 = MessageDigest.getInstance("SHA-1");
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public int generateClassStorageId(Class<?> type, String name) {
        return this.getStorageId("class:" + name);
    }

    @Override
    public int generateCompositeIndexStorageId(Class<?> type, String name, int[] fields) {
        return this.getStorageId("index:" + name);
    }

    @Override
    public int generateFieldStorageId(Method getter, String name) {
        return this.getStorageId("field:" + name);
    }

    @Override
    public int generateSetElementStorageId(Method getter, String name) {
        return this.getStorageId("field:" + name + "." + SetField.ELEMENT_FIELD_NAME);
    }

    @Override
    public int generateListElementStorageId(Method getter, String name) {
        return this.getStorageId("field:" + name + "." + ListField.ELEMENT_FIELD_NAME);
    }

    @Override
    public int generateMapKeyStorageId(Method getter, String name) {
        return this.getStorageId("field:" + name + "." + MapField.KEY_FIELD_NAME);
    }

    @Override
    public int generateMapValueStorageId(Method getter, String name) {
        return this.getStorageId("field:" + name + "." + MapField.VALUE_FIELD_NAME);
    }

    private int getStorageId(String string) {
        this.sha1.reset();
        final byte[] digest = this.sha1.digest(string.getBytes(Charset.forName("UTF-8")));
        int value = 0;
        for (int i = 0; i < 4; i++)
            value = (value << 8) | (digest[i] & 0xff);
        value &= 0x7fffffff;
        return (value % (MAX_STORAGE_ID - MIN_STORAGE_ID)) + MIN_STORAGE_ID;
    }
}

