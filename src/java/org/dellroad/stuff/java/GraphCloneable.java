
/*
 * Copyright (C) 2011 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.dellroad.stuff.java;

/**
 * Implemented by objects in an object graph that support <i>graph cloning</i>, i.e., deep copies without
 * duplicates and preserving reference topology.
 *
 * <p>
 * Graph cloning creates a copy of an object graph that, like a normal deep copy, contains no references to objects in the
 * original object graph (except for immutables), but in addition, each {@link GraphCloneable} object is only copied once.
 * As a result the copy graph has the same reference topology as the original graph (with respect to all the
 * {@link GraphCloneable}s). In particular, reference cycles among {@link GraphCloneable}s are preserved and do not
 * cause infinite loops.
 * </p>
 *
 * <p>
 * Graph cloning operates similar to a deep copy operation, except that graph cloning uses a {@link GraphCloneRegistry} to keep
 * track of new objects as they are created. When each object's {@link GraphCloneable} references are copied, the
 * {@link GraphCloneRegistry} is used to check whether the referred-to objects have already been copied, and if so, the existing
 * copy is used. This requires that implementations {@linkplain GraphCloneRegistry#setGraphClone register} their clones
 * prior to {@linkplain GraphCloneRegistry#getGraphClone recursing} on any {@link GraphCloneable} fields.
 * </p>
 *
 * <p>
 * The net effect is equivalent to serializing and then deserializing the entire object graph, but without the overhead,
 * losing {@code transient} values, and other issues.
 * </p>
 *
 * <p>
 * Here is an example of a class properly implementing this interface:
 *  <pre>
 *  // We implement Cloneable so super.clone() will work
 *  public class Person implements Cloneable, GraphCloneable {
 *
 *      // Regular fields
 *      private int age;
 *      private String lastName;
 *      private String firstName;
 *      private List&lt;String&gt; nicknames = new ArrayList&lt;String&gt;();
 *
 *      // GraphCloneable fields - values may be null and/or even refer back to me
 *      private Person spouse;
 *      private List&lt;Person&gt; friends = new ArrayList&lt;Person&gt;();
 *
 *      // Getters &amp; setters go here...
 *
 *      // Our implementation of the GraphCloneable interface
 *      &#64;Override
 *      public void createGraphClone(GraphCloneRegistry registry) throws CloneNotSupportedException {
 *
 *          // Create clone and register it with the registry
 *          final Person clone = (Person)super.clone();
 *          registry.setGraphClone(clone);                      // the registry knows who we are
 *
 *          // Deep copy any regular fields not already handled by super.clone()
 *          clone.nicknames = new ArrayList&lt;String&gt;(this.nicknames);
 *
 *          // Now copy GraphCloneable fields using registry.getGraphClone()
 *          clone.spouse = registry.getGraphClone(this.spouse);
 *          clone.friends = new ArrayList&lt;Person&gt;(this.friends.size());
 *          for (Person friend : this.friends)
 *              clone.friends.add(registry.getGraphClone(friend));
 *      }
 *  }
 *  </pre>
 * </p>
 *
 * <p>
 * To graph clone the object graph rooted at {@code root}, you would do this:
 *  <pre>
 *      new GraphCloneRegistry().getGraphClone(root);
 *  </pre>
 * </p>
 *
 * @see GraphCloneRegistry
 */
public interface GraphCloneable {

    /**
     * Create a graph clone of this instance and register it with the given {@link GraphCloneRegistry}.
     *
     * <p>
     * This method should perform a normal "deep copy" operation, but with the following changes:
     * <ul>
     *  <li>
     *      The new clone must be {@linkplain GraphCloneRegistry#setGraphClone registered} with the given
     *      {@link GraphCloneRegistry} prior to {@linkplain GraphCloneRegistry#getGraphClone recursing}
     *      on any {@link GraphCloneable} fields.
     *  </li>
     *  <li>
     *      All {@link GraphCloneable} fields must be copied via {@link GraphCloneRegistry#getGraphClone}.
     *  </li>
     * </ul>
     * </p>
     *
     * <p>
     * The most efficient implementation of this method often involves declaring the class to implement {@link Cloneable}
     * and starting with an invocation of {@link Object#clone super.clone()}. For that reason, this method is declared
     * to throw {@link CloneNotSupportedException} as a coding convenience; if a {@link CloneNotSupportedException} is
     * actually thrown, it will trigger a {@link RuntimeException}.
     * </p>
     *
     * <p>
     * This method will only be invoked once for any instance during a graph cloning operation.
     * </p>
     *
     * @throws NullPointerException if {@code registry} is null
     * @throws CloneNotSupportedException declared so implementors can invoke {@link Object#clone super.clone()}
     *  directly; if thrown, it will trigger a {@link RuntimeException}
     */
    void createGraphClone(GraphCloneRegistry registry) throws CloneNotSupportedException;
}

