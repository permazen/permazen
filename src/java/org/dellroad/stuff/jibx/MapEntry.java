
/*
 * Copyright (C) 2011 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.dellroad.stuff.jibx;

import java.util.Iterator;
import java.util.Map;

/**
 * Utility class for modeling {@link Map} properties in JiBX.
 *
 * <p>
 * For example, suppose you have a class {@code Company} with a property named {@code directory} that has
 * type {@code Map<String, Person>}.  Then you would define these methods in {@code Company.java}:
 *
 * <blockquote><code>
 * // Getter and setter
 * public Map<String, Person> getDirectory() {
 *     return this.directory;
 * }
 * public void setDirectory(Map<String, Person> directory) {
 *     this.directory = directory;
 * }
 *
 * // JiBX "add-method"
 * public void addDirectoryEntry(MapEntry<String, Person> entry) {
 *     MapEntry.add(this.directory, entry);
 * }
 *
 * // JiBX "iter-method"
 * public Iterator<MapEntry<String, Person> iterateDirectory() {
 *     return MapEntry.iterate(this.directory);
 * }
 * </code></blockquote>
 *
 * <p>
 * Then in your JiBX binding definition, you would do something like this:
 *
 * <blockquote><code>
 * &lt;binding package="com.example"&gt;
 *
 *     &lt;!-- Include XML mapping definition for a Person object --&gt;
 *     &lt;include path="person.xml"/&gt;
 *
 *     &lt;!-- Define XML mapping for one entry in the directory map --&gt;
 *     &lt;mapping abstract="true" type-name="directory_entry" class="org.dellroad.stuff.jibx.MapEntry"&gt;
 *         &lt;value name="name" field="key" type="java.lang.String" style="attribute"/&gt;
 *         &lt;structure name="person" map-as="person"/&gt;
 *     &lt;/mapping&gt;
 *
 *     &lt;!-- Define XML mapping for a Company object --&gt;
 *     &lt;mapping abstract="true" type-name="company" class="com.example.Company"&gt;
 *         &lt;collection name="directory" item-type="org.dellroad.stuff.jibx.MapEntry"
 *           add-method="addDirectoryEntry" iter-method="iterateDirectory"&gt;
 *             &lt;structure name="entry" map-as="directory_entry"/&gt;
 *         &lt;/collection&gt;
 *         &lt;!-- other properties... --&gt;
 *     &lt;/mapping&gt;
 * &lt;/binding&gt;
 * </code></blockquote>
 *
 * The resulting XML would look like this:
 * <blockquote><code>
 * &lt;company&gt;
 *     &lt;directory&gt;
 *         &lt;entry name="George Washington"&gt;
 *             &lt;person&gt;
 *                  &lt;!-- properties of George Washington... --&gt;
 *             &lt;/person&gt;
 *         &lt;/entry&gt;
 *         &lt;entry name="Besty Ross"&gt;
 *             &lt;person&gt;
 *                  &lt;!-- properties of Besty Ross... --&gt;
 *             &lt;/person&gt;
 *         &lt;/entry&gt;
 *     &lt;/directory&gt;
 *     &lt;!-- other properties... --&gt;
 * &lt;/company&gt;
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
     * The new entry is added to the map, replacing any existing entry with the same key.
     *
     * @param map map to which to add an new entry
     * @param entry new entry to add
     */
    public static <K, V> void add(Map<K, V> map, MapEntry<? extends K, ? extends V> entry) {
        map.put(entry.getKey(), entry.getValue());
    }
}

