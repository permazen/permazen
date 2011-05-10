
/*
 * Copyright (C) 2011 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.dellroad.stuff.string;

import java.beans.BeanInfo;
import java.beans.IndexedPropertyDescriptor;
import java.beans.IntrospectionException;
import java.beans.Introspector;
import java.beans.PropertyDescriptor;
import java.lang.reflect.Method;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

import org.dellroad.stuff.java.Primitive;

/**
 * Parses strings using regular expressions into new instances of some class by parsing substrings.
 * Primitive and String values are handled automatically. Other property types can be handled by
 * overriding {@link #setProperty}.
 *
 * <a name="namedgroup"/>
 * <p>
 * This class supports parsing using a <i>named group regular expression</i>, which is pattern string
 * using normal {@link Pattern} regular expression syntax with one additional grouping construct of the
 * form <code>({property}...)</code>, allowing the Java bean property name to be specified inside the
 * curly braces at the start of a grouped subexpression.
 *
 * <p>
 * Instances of this class are immutable and thread-safe.
 */
public class SimpleObjectParser<T> {

    private final Class<T> targetClass;
    private final HashMap<String, PropertyDescriptor> propertyMap = new HashMap<String, PropertyDescriptor>();

    /**
     * Constructor.
     *
     * @param targetClass type of target object we will be parsing
     */
    public SimpleObjectParser(Class<T> targetClass) {
        this.targetClass = targetClass;
        this.buildPropertyMap();
    }

    /**
     * Get the target class.
     */
    public Class<T> getTargetClass() {
        return this.targetClass;
    }

    /**
     * Same as {@link #parse(Object, String, String, boolean)} but this method creates the target instance using
     * the target type's default constructor.
     *
     * @throws RuntimeException if a new target instance cannot be created using the default constructor
     * @since 1.0.85
     */
    public T parse(String text, String regex, boolean allowSubstringMatch) {
        T target;
        try {
            target = this.targetClass.newInstance();
        } catch (Exception e) {
            throw new RuntimeException("can't create instance of " + this.targetClass + " using default constructor", e);
        }
        return this.parse(target, text, regex, allowSubstringMatch);
    }

    /**
     * Parse the given text using the provided <i>named group regular expression</i>.
     *
     * <p>
     * This method assumes the following about {@code regex}:
     * <ul>
     * <li>All instances of an opening parenthesis not preceded by a backslash are actual grouped sub-expressions</li>
     * <li>In particular, all instances of substrings like <code>({foo}</code> are actual named group sub-expressions</li>
     * </ul>
     *
     * @param target              target instance
     * @param text                string to parse
     * @param regex               named group regular expression containing object property names
     * @param allowSubstringMatch if false, entire text must match, otherwise only a (the first) substring need match
     * @return parsed object or null if parse fails
     * @throws PatternSyntaxException if the regular expression with the named group property names removed is invalid
     * @throws PatternSyntaxException if this method cannot successfully parse the regular expression
     * @throws IllegalArgumentException if a named group specfies a property that is not a parseable
     *                                  property of this instance's target class
     * @since 1.0.95
     */
    public T parse(T target, String text, String regex, boolean allowSubstringMatch) {

        // Scan regular expression for named sub-groups and parse them out
        HashMap<Integer, String> patternMap = new HashMap<Integer, String>();
        StringBuilder buf = new StringBuilder(regex.length());
        Pattern namedGroup = Pattern.compile("\\(\\{(\\p{javaJavaIdentifierStart}\\p{javaJavaIdentifierPart}*)\\}");
        Matcher matcher = namedGroup.matcher(regex);
        int pos = 0;
        int groupCount = 0;
        while (true) {
            int match = matcher.find(pos) ? matcher.start() : regex.length();
            String chunk = regex.substring(pos, match);
            for (int i = 0; i < chunk.length(); i++) {
                if (chunk.charAt(i) == '('
                  && (i == 0 || chunk.charAt(i - 1) != '\\')
                  && (i == chunk.length() - 1 || chunk.charAt(i + 1) != '?'))
                    groupCount++;
            }
            buf.append(chunk);
            if (match == regex.length())
                break;
            buf.append('(');
            patternMap.put(++groupCount, matcher.group(1));
            pos = matcher.end();
        }

        // Sanity check our parse attempt
        Pattern pattern = Pattern.compile(buf.toString());
        int numGroups = pattern.matcher("").groupCount();
        if (numGroups != groupCount) {
            throw new PatternSyntaxException("the given regular expression is not supported (counted "
              + groupCount + " != " + numGroups + " groups)", regex, 0);
        }

        // Proceed
        return this.parse(target, text, pattern, patternMap, allowSubstringMatch);
    }

    /**
     * Same as {@link #parse(Object, String, Pattern, Map, boolean)} but this method creates the target instance using
     * the target type's default constructor.
     *
     * @throws RuntimeException if a new target instance cannot be created using the default constructor
     * @since 1.0.85
     */
    public T parse(String text, Pattern pattern, Map<Integer, String> patternMap, boolean allowSubstringMatch) {
        T target;
        try {
            target = this.targetClass.newInstance();
        } catch (Exception e) {
            throw new RuntimeException("can't create instance of " + this.targetClass + " using default constructor", e);
        }
        return this.parse(target, text, pattern, patternMap, allowSubstringMatch);
    }

    /**
     * Parse the given text using the provided pattern and mapping from pattern sub-group to Java bean property name.
     *
     * @param target              target instance
     * @param text                string to parse
     * @param pattern             pattern with substring matching groups that match object properties
     * @param patternMap          mapping from pattern substring group index to object property name
     * @param allowSubstringMatch if false, entire text must match, otherwise only a (the first) substring need match
     * @return parsed object or null if parse fails
     * @throws IllegalArgumentException if the map contains a property that is not a parseable
     *                                  property of this instance's target class
     * @throws IllegalArgumentException if a subgroup index key in patternMap is out of bounds
     * @since 1.0.95
     */
    public T parse(T target, String text, Pattern pattern, Map<Integer, String> patternMap, boolean allowSubstringMatch) {

        // Compose given map with target class' property map
        HashMap<Integer, PropertyDescriptor> subgroupMap = new HashMap<Integer, PropertyDescriptor>();
        for (Map.Entry<Integer, String> entry : patternMap.entrySet()) {
            String propName = entry.getValue();
            PropertyDescriptor property = this.propertyMap.get(propName);
            if (property == null)
                throw new IllegalArgumentException("parseable property \"" + propName + "\" not found in " + this.targetClass);
            subgroupMap.put(entry.getKey(), property);
        }

        // Attempt to match the string
        Matcher matcher = pattern.matcher(text);
        boolean matches = allowSubstringMatch ? matcher.find() : matcher.matches();
        if (!matches)
            return null;

        // Set fields based on matching substrings
        for (Map.Entry<Integer, PropertyDescriptor> entry : subgroupMap.entrySet()) {

            // Get substring
            String substring;
            try {
                substring = matcher.group(entry.getKey());
            } catch (IndexOutOfBoundsException e) {
                throw new IllegalArgumentException(
                  "regex subgroup " + entry.getKey() + " does not exist in pattern `" + pattern + "'");
            }

            // If substring was not matched, don't set property
            if (substring == null)
                continue;

            // Set property from substring
            this.setProperty(target, entry.getValue(), substring);
        }

        // Post-process
        this.postProcess(target);

        // Done
        return target;
    }

    /**
     * Get the mapping from property name to setter method.
     */
    public Map<String, PropertyDescriptor> getPropertyMap() {
        return Collections.unmodifiableMap(this.propertyMap);
    }

    /**
     * Set a property value.
     * <p/>
     * <p>
     * The implementation in {@link SimpleObjectParser} simply invokes {@link #setSimpleProperty}.
     * Other property types can be handled by overriding this method.
     * </p>
     *
     * @param obj       newly created instance
     * @param property  descriptor for the property being set
     * @param substring matched substring
     * @throws IllegalArgumentException if substring cannot be successfully parsed
     * @throws IllegalArgumentException if an exception is thrown attempting to set the property
     */
    public void setProperty(T obj, PropertyDescriptor property, String substring) {
        this.setSimpleProperty(obj, property, substring);
    }

    /**
     * Set a primitive or string property value.
     * <p/>
     * <p>
     * The implementation in {@link SimpleObjectParser} handles primitives using the corresponding
     * {@code valueOf} method; String values are handled by setting the value directly.
     * </p>
     *
     * @throws IllegalArgumentException if property is not a primitive or String property.
     * @throws IllegalArgumentException if substring cannot be successfully parsed (if primitive)
     * @throws IllegalArgumentException if an exception is thrown attempting to set the property
     */
    public void setSimpleProperty(T obj, PropertyDescriptor property, String substring) {

        // Parse substring
        Object value;
        if (property.getPropertyType() == String.class)
            value = substring;
        else {
            Primitive prim = Primitive.get(property.getPropertyType());
            if (prim == null) {
                throw new IllegalArgumentException(
                  "property `" + property.getName() + "' of " + this.targetClass + " is not a primitive or String");
            }
            value = prim.parseValue(substring);
        }

        // Set value
        try {
            property.getWriteMethod().invoke(obj, value);
        } catch (Exception e) {
            throw new IllegalArgumentException("can't set property `" + property.getName() + "' of " + this.targetClass, e);
        }
    }

    /**
     * Post-process newly created instances. The instance's properties will have already been
     * set by a successful parse.
     * <p/>
     * <p>
     * The implementation in {@link SimpleObjectParser} does nothing. Subclasses may override if needed.
     * </p>
     */
    protected void postProcess(T obj) {
    }

    private void buildPropertyMap() {

        // Introspect target class
        BeanInfo beanInfo;
        try {
            beanInfo = Introspector.getBeanInfo(this.targetClass);
        } catch (IntrospectionException e) {
            throw new RuntimeException(e);
        }

        // Build map from property name -> setter method
        for (PropertyDescriptor property : beanInfo.getPropertyDescriptors()) {
            if (property instanceof IndexedPropertyDescriptor)
                continue;
            Method setter = property.getWriteMethod();
            if (setter == null)
                continue;
            Class<?> type = property.getPropertyType();
            this.propertyMap.put(property.getName(), property);
        }
    }
}

