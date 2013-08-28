
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
 * original object graph (except for immutables), but in addition, each {@link GraphClonable} in the graph is only copied once.
 * As a result the copy graph has the same reference topology as the original graph (with respect to all the
 * {@link GraphClonable}s). In particular, no {@link GraphClonable} is copied more than once, and reference cycles among
 * {@link GraphClonable}s are preserved.
 * </p>
 *
 * <p>
 * Graph cloning operates similar to a deep clone operation, except that graph cloning uses a {@link GraphCloner} to keep
 * track of new objects as they are created. When each object's {@link GraphClonable} references are copied, the
 * {@link GraphCloner} is used to check whether the referred-to objects have already been copied, and if so, the existing
 * copy is used. This requires a two-step process: first {@linkplain #createGraphClone create the clone} so it can be
 * immediately recorded, then {@linkplain #copyGraphClonables recurse} on {@link GraphClonable} fields.
 * </p>
 *
 * <p>
 * The net effect is equivalent to serializing and then deserializing the entire object graph, yet also preserving
 * reference topology, and executing as fast as native Java cloning.
 * </p>
 *
 * <p>
 * Here is an example.
 *  <pre>
 *  public class Person implements Clonable, GraphClonable&lt;Person&gt; {
 *
 *      // Regular fields
 *      private String lastName;
 *      private String firstName;
 *      private int age;
 *
 *      // GraphClonable fields - values may be null and/or even refer back to me!
 *      private Person spouse;
 *      private final ArrayList&lt;Person&gt; children = new ArrayList&lt;Person&gt;();
 *
 *      ...
 *
 *      &#64;Override
 *      public Person createGraphClone() {
 *          return this.clone();
 *      }
 *
 *      &#64;Override
 *      public void copyGraphClonables(Person clone) {
 *          clone.spouse = GraphCloner.getGraphClone(this.spouse);
 *          clone.children.clear();
 *          clone.children.ensureCapacity(this.children.size());
 *          for (Person child : this.children)
 *              clone.children.add(GraphCloner.getGraphClone(child));
 *      }
 *
 *      // A normal shallow clone operation
 *      &#64;Override
 *      public Person clone() {
 *          try {
 *              return super.clone();
 *          } catch (CloneNotSupportedException e) {
 *              throw new RuntimeException(e);
 *          }
 *      }
 *
 *      // Example of how to create a graph clone
 *      public Person graphClone() {
 *          return GraphCloner.getGraphClone(this);
 *      }
 *  }
 *  </pre>
 * </p>
 *
 * @see GraphCloner
 */
public interface GraphClonable<T extends GraphClonable<T>> {

    /**
     * Create a deep copy of this instance, but leave any {@link GraphClonable} fields uncopied.
     * That is, this method should <b>not</b> recurse on any {@link GraphClonable} fields.
     *
     * <p>
     * Other fields should be deep copied, as appropriate for a normal deep copy operation
     * (alternately, such fields may be copied later in {@link #copyGraphClonables}).
     * </p>
     *
     * <p>
     * This method will only be invoked once for any instance during a graph cloning operation.
     * </p>
     */
    T createGraphClone();

    /**
     * Graph clone all {@link GraphClonable} references in this instance and assign them to the given instance.
     * All {@link GraphClonable} references should be copied using {@link GraphCloner#getGraphClone}.
     *
     * <p>
     * This method will only be invoked once for any instance during a graph cloning operation.
     * </p>
     *
     * @param clone newly created unique clone of this instance
     */
    void copyGraphClonables(T clone);
}

