
/*
 * Copyright (C) 2011 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.dellroad.stuff.jibx;

import java.util.Iterator;
import java.util.Map;

import org.jibx.runtime.JiBXParseException;

/**
 * Utility class for modeling {@link Map} properties in JiBX.
 *
 * <p>
 * For example, suppose you have a class {@code Company} with a property named {@code directory} that has
 * type {@code Map<String, Person>}.  Then you would define these methods in {@code Company.java}:
 *
 * <blockquote><pre>
 * // Getter and setter
 * public Map&lt;String, Person&gt; getDirectory() {
 *     return this.directory;
 * }
 * public void setDirectory(Map&lt;String, Person&gt; directory) {
 *     this.directory = directory;
 * }
 *
 * // JiBX "add-method"
 * public void addDirectoryEntry(MapEntry&lt;String, Person&gt; direntry) {
 *     MapEntry.add(this.directory, direntry);
 * }
 *
 * // JiBX "iter-method"
 * public Iterator&lt;MapEntry&lt;String, Person&gt;&gt; iterateDirectory() {
 *     return MapEntry.iterate(this.directory);
 * }
 * </pre></blockquote>
 *
 * <p>
 * Then in your JiBX binding definition, you would do something like this:
 *
 * <blockquote><pre>
 * &lt;binding package="com.example"&gt;
 *
 *     &lt;!-- Include XML mapping definition for a Person object with type-name "person" --&gt;
 *     &lt;include path="person.xml"/&gt;
 *
 *     &lt;!-- Define XML mapping for one entry in the directory map --&gt;
 *     &lt;mapping abstract="true" type-name="directory_entry" class="org.dellroad.stuff.jibx.MapEntry"&gt;
 *         &lt;value name="name" field="key" type="java.lang.String" style="attribute"/&gt;
 *         &lt;structure name="Person" field="value" map-as="person"/&gt;
 *     &lt;/mapping&gt;
 *
 *     &lt;!-- Define XML mapping for a Company object --&gt;
 *     &lt;mapping abstract="true" type-name="company" class="com.example.Company"&gt;
 *         &lt;collection name="Directory" item-type="org.dellroad.stuff.jibx.MapEntry"
 *           add-method="addDirectoryEntry" iter-method="iterateDirectory"&gt;
 *             &lt;structure name="DirectoryEntry" map-as="directory_entry"/&gt;
 *         &lt;/collection&gt;
 *         &lt;!-- other properties... --&gt;
 *     &lt;/mapping&gt;
 * &lt;/binding&gt;
 * </pre></blockquote>
 *
 * Then the resulting XML would look something like this:
 * <blockquote><pre>
 * &lt;Company&gt;
 *     &lt;Directory&gt;
 *         &lt;DirectoryEntry name="George Washington"&gt;
 *             &lt;Person&gt;
 *                  &lt;!-- properties of George Washington... --&gt;
 *             &lt;/Person&gt;
 *         &lt;/DirectoryEntry&gt;
 *         &lt;DirectoryEntry name="Besty Ross"&gt;
 *             &lt;Person&gt;
 *                  &lt;!-- properties of Besty Ross... --&gt;
 *             &lt;/Person&gt;
 *         &lt;/DirectoryEntry&gt;
 *     &lt;/Directory&gt;
 *     &lt;!-- other properties... --&gt;
 * &lt;/Company&gt;
 * </pre></blockquote>
 */
public class MapEntry<K, V> {

    private K key;
    private V value;

    /**
     * Default constructor. Initializes both key and value to {@code null}.
     */
    public MapEntry() {
    }

    /**
     * Primary constructor.
     *
     * @param key map entry key
     * @param value map entry value
     */
    public MapEntry(K key, V value) {
        this.key = key;
        this.value = value;
    }

    /**
     * Get this map entry's key.
     */
    public K getKey() {
        return this.key;
    }
    public void setKey(K key) {
        this.key = key;
    }

    /**
     * Get this map entry's value.
     */
    public V getValue() {
        return this.value;
    }
    public void setValue(V value) {
        this.value = value;
    }

    /**
     * Helper method intended to be used by a custom JiBX "iter-method".
     * This method returns an iterator that iterates over all entries in the given map.
     *
     * @param map map to iterate
     * @return map entry iterator
     */
    public static <K, V> Iterator<MapEntry<K, V>> iterate(Map<K, V> map) {
        final Iterator<Map.Entry<K, V>> entryIterator = map.entrySet().iterator();
        return new Iterator<MapEntry<K, V>>() {

            @Override
            public boolean hasNext() {
                return entryIterator.hasNext();
            }

            @Override
            public MapEntry<K, V> next() {
                Map.Entry<K, V> entry = entryIterator.next();
                return new MapEntry<K, V>(entry.getKey(), entry.getValue());
            }

            @Override
            public void remove() {
                entryIterator.remove();
            }
        };
    }

    /**
     * Helper method intended to be used by a custom JiBX "add-method".
     * If there is an existing entry with the same key, a {@link JiBXParseException} is thrown.
     *
     * @param map map to which to add an new entry
     * @param entry new entry to add
     * @throws JiBXParseException if the map already contains an entry with the given key
     */
    public static <K, V> void add(Map<K, V> map, MapEntry<? extends K, ? extends V> entry) throws JiBXParseException {
        MapEntry.add(map, entry, false);
    }

    /**
     * Helper method intended to be used by a custom JiBX "add-method".
     *
     * @param map map to which to add an new entry
     * @param entry new entry to add
     * @param allowDuplicate {@code true} to replace any existing entry having the same key,
     *  or {@code false} to throw a {@link JiBXParseException} if there is an existing entry
     * @throws JiBXParseException if {@code allowDuplicate} is {@code false} and an entry
     *  with the same key already exists in {@code map}
     */
    public static <K, V> void add(Map<K, V> map, MapEntry<? extends K, ? extends V> entry, boolean allowDuplicate)
      throws JiBXParseException {
        K key = entry.getKey();
        V value = entry.getValue();
        if (!allowDuplicate && map.containsKey(key))
            throw new JiBXParseException("duplicate key in map", "" + key);
        map.put(key, value);
    }
}

