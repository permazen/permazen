
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
 * Utility class that makes it slightly easier to model {@link Map} properties in JiBX.
 * This class can be used to represent entries in the map, each of which is modeled in XML as a separate XML element.
 *
 * <p>
 * For example, suppose you have a class {@code Company} and want to add a {@code directory} property that has
 * type {@code Map<String, Person>}:
 * <blockquote><pre>
 * public class Company {
 *     private Map&lt;String, Person&gt; directory = new HashMap&lt;String, Person&gt;();
 *
 *     // Getter and setter for the "directory" property
 *     public Map&lt;String, Person&gt; getDirectory() {
 *         return this.directory;
 *     }
 *     public void setDirectory(Map&lt;String, Person&gt; directory) {
 *         this.directory = directory;
 *     }
 * }
 * </pre></blockquote>
 *
 * <p>
 * Because the JiBX binding process modifies class files, you first need to create your own subclass of {@link MapEntry}
 * that can be modified. In this example, we'll use an inner class of {@code Company}. In addition, you also need to add
 * JiBX "add-method" and "iter-method" helper methods. The resulting new code might look like this:
 * <blockquote><pre>
 *     // JiBX holder for a single entry in the Directory map
 *     public static class DirectoryEntry extends MapEntry&lt;String, Person&gt; {
 *        public String getKey()   { return super.getKey();   }   // JiBX requires exact return types
 *        public Person getValue() { return super.getValue(); }   // JiBX requires exact return types
 *     }
 *
 *     // JiBX "add-method" that adds a new entry to the directory
 *     void addDirectoryEntry(DirectoryEntry entry) throws JiBXParseException {
 *         MapEntry.add(this.directory, entry);
 *     }
 *
 *     // JiBX "iter-method" that iterates all entries in the directory
 *     Iterator&lt;DirectoryEntry&gt; iterateDirectoryEntries() {
 *         return MapEntry.iterate(this.directory, DirectoryEntry.class);
 *     }
 * </pre></blockquote>
 *
 * <p>
 * Then in your JiBX binding definition, you would do something like this:
 * <blockquote><pre>
 * &lt;binding package="com.example"&gt;
 *
 *     &lt;!-- Include XML mapping definition for a Person object (having type-name "person") --&gt;
 *     &lt;include path="person.xml"/&gt;
 *
 *     &lt;!-- Define the XML mapping for one entry in the "directory" map --&gt;
 *     &lt;mapping abstract="true" type-name="directory_entry" class="com.example.Company$DirectoryEntry"&gt;
 *         &lt;value name="name" get-method="getKey" set-method="setKey" type="java.lang.String" style="attribute"/&gt;
 *         &lt;structure name="Person" get-method="getValue" set-method="setValue" map-as="person"/&gt;
 *     &lt;/mapping&gt;
 *
 *     &lt;!-- Define XML mapping for a Company object --&gt;
 *     &lt;mapping abstract="true" type-name="company" class="com.example.Company"&gt;
 *         &lt;collection name="Directory" item-type="com.example.Company$DirectoryEntry"
 *           add-method="addDirectoryEntry" iter-method="iterateDirectoryEntries"&gt;
 *             &lt;structure name="DirectoryEntry" map-as="directory_entry"/&gt;
 *         &lt;/collection&gt;
 *         &lt;!-- other properties... --&gt;
 *     &lt;/mapping&gt;
 * &lt;/binding&gt;
 * </pre></blockquote>
 *
 * Then the resulting XML would end up looking something like this:
 * <blockquote><pre>
 * &lt;Company&gt;
 *     &lt;Directory&gt;
 *         &lt;DirectoryEntry name="George Washington"&gt;
 *             &lt;Person&gt;
 *                  &lt;!-- properties of George Washington... --&gt;
 *             &lt;/Person&gt;
 *         &lt;/DirectoryEntry&gt;
 *         &lt;DirectoryEntry name="Betsy Ross"&gt;
 *             &lt;Person&gt;
 *                  &lt;!-- properties of Betsy Ross... --&gt;
 *             &lt;/Person&gt;
 *         &lt;/DirectoryEntry&gt;
 *     &lt;/Directory&gt;
 *     &lt;!-- other properties... --&gt;
 * &lt;/Company&gt;
 * </pre></blockquote>
 *
 * <p>
 * Note that during unmarshalling, the <code>Map</code> itself is not created; it is expected to already exist
 * and be empty. This will be the case if you provide a field initializer as in the example above.
 *
 * <p>
 * The map keys are not constrained to being simple values: for complex keys, just adjust the mapping for the
 * {@code DirectoryEntry} structure accordingly.
 */
public class MapEntry<K, V> {

    private K key;
    private V value;

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
     * @param <K> type of map keys
     * @param <V> type of map values
     * @param map map to iterate
     * @param entryClass the subclass of {@link MapEntry} used for iterated elements; must have a default constructor
     * @return map entry iterator
     */
    public static <K, V, E extends MapEntry<K, V>> Iterator<E> iterate(Map<K, V> map, final Class<E> entryClass) {
        final Iterator<Map.Entry<K, V>> entryIterator = map.entrySet().iterator();
        return new Iterator<E>() {

            @Override
            public boolean hasNext() {
                return entryIterator.hasNext();
            }

            @Override
            public E next() {
                Map.Entry<K, V> entry = entryIterator.next();
                E mapEntry;
                try {
                    mapEntry = entryClass.newInstance();
                } catch (Exception e) {
                    throw new RuntimeException("unexpected exception", e);
                }
                mapEntry.setKey(entry.getKey());
                mapEntry.setValue(entry.getValue());
                return mapEntry;
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

