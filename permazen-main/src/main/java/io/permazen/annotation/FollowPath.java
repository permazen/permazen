
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.annotation;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Java annotation for declaring methods that should return objects found by traversing a {@link io.permazen.ReferencePath}.
 *
 * <p>
 * This annotation can be used as a convenience to let JSimpleDB auto-generate reference path traversal code.
 * A common use case is inverting references, e.g., from a parent back to a child in a one-to-many relationship.
 *
 * <p>
 * Annotating an abstract method with {@code @FollowPath("some.path")} is just short-hand for providing an implementation
 * that creates and traverses the {@linkplain io.permazen.ReferencePath reference path} {@code "some.path"}.
 * See {@link io.permazen.ReferencePath} for details on reference paths. The specified reference path is assumed to not
 * have any {@linkplain io.permazen.ReferencePath#getTargetField target field}.
 *
 * <p>
 * For example, the following declaration:
 * <pre>
 *      &#64;FollowPath("^TreeNode:parent^")
 *      public abstract NavigableSet&lt;TreeNode&gt; getChildren();
 * </pre>
 * is functionally equivalent to, and slightly more efficient than:
 * <pre>
 *
 *      public NavigableSet&lt;TreeNode&gt; getChildren() {
 *          final JSimpleDB jdb = /* the JSimpleDB instance associated with this instance *&#47;
 *          final ReferencePath path = jdb.parseReferencePath(this.getClass(), "^TreeNode:parent^", false);
 *          return jdb.followReferencePath(path, Collections.singleton(this));
 *      }
 * </pre>
 *
 * <p><b>Method Return Type</b></p>
 *
 * <p>
 * The annotated method should return {@link java.util.NavigableSet NavigableSet&lt;T&gt;}, where {@code T} is any super-type of
 * the type(s) at the other end of the reference path.
 *
 * <p>
 * However, if {@code firstOnly} is true then the method returns only the first object found (if any), and the method's
 * return type should instead be {@link java.util.Optional}{@code <T>}; this is always appropriate when the reference path
 * contains only forward traversals of many-to-one relationships.
 *
 * <p>
 * For example:
 * <pre>
 *  &#64;PermazenType
 *  public interface TreeNode extends JObject {
 *
 *      /**
 *       * Get the parent of this node, or null if node is a root.
 *       *&#47;
 *      TreeNode getParent();
 *      void setParent(TreeNode parent);
 *
 *      /**
 *       * Get the children of this node.
 *       *&#47;
 *      <b>&#64;FollowPath("^TreeNode:parent^")</b>
 *      NavigableSet&lt;TreeNode&gt; getChildren();
 *
 *      /**
 *       * Get the siblings of this node (including this node).
 *       *&#47;
 *      <b>&#64;FollowPath("parent.^TreeNode:parent^")</b>
 *      NavigableSet&lt;TreeNode&gt; getSiblings();
 *
 *      /**
 *       * Get the grandparent of this node.
 *       *&#47;
 *      <b>&#64;FollowPath(value = "parent.parent", firstOnly = true)</b>
 *      Optional&lt;TreeNode&gt; getGrandparent();
 *  }
 * </pre>
 *
 * <p><b>Inverse Paths</b></p>
 *
 * <p>
 * Inverse paths may be specified using the combination of {@link #inverseOf} and {@link #startingFrom} properties.
 * So the following two declarations are functionally equivalent:
 * <pre>
 *      /**
 *       * Get the children of this node.
 *       *&#47;
 *      <b>&#64;FollowPath("^TreeNode:parent^")</b>
 *      NavigableSet&lt;TreeNode&gt; getChildren();
 *
 *      /**
 *       * Get the children of this node.
 *       *&#47;
 *      <b>&#64;FollowPath(inverseOf = "parent", startingFrom = TreeNode.class)</b>
 *      NavigableSet&lt;TreeNode&gt; getChildren();
 * </pre>
 *
 * <p><b>Meta-Annotations</b></p>
 *
 * <p>
 * This annotation may be configured indirectly as a Spring
 * <a href="https://docs.spring.io/spring/docs/current/spring-framework-reference/htmlsingle/#beans-meta-annotations">meta-annotation</a>
 * when {@code spring-core} is on the classpath.
 */
@Retention(RetentionPolicy.RUNTIME)
@Target({ ElementType.ANNOTATION_TYPE, ElementType.METHOD })
@Documented
public @interface FollowPath {

    /**
     * The reference path to follow in the forward direction.
     *
     * <p>
     * The starting type for the reference path is the class containing the annotated method.
     *
     * <p>
     * If this property is used, both of {@link #inverseOf} and {@link #startingFrom} must be left unset.
     *
     * @return the reference path to traverse in the forward direction
     */
    String value() default "";

    /**
     * The reference path to follow in the inverse direction.
     *
     * <p>
     * If this property is used, {@link #startingFrom} must also be specified and {@link #value} must be left unset.
     *
     * @return the reference path to traverse in the inverse direction
     */
    String inverseOf() default "";

    /**
     * The starting model type for the reference path to follow in the inverse direction.
     *
     * <p>
     * When an inverse reference path is specified, the starting type for the reference path is not implied, because
     * the path starts from the "remote end", instead of from the class containing the annotated method.
     * Therefore, it must be explicitly specified via this property.
     *
     * <p>
     * If this property is used, {@link #inverseOf} must also be specified and {@link #value} must be left unset.
     *
     * @return the starting model type for the inverted reference path
     */
    Class<?> startingFrom() default void.class;

    /**
     * Whether only the first, if any, of the set of objects should be returned, or the entire set.
     *
     * <p>
     * If this property is true, then the annotated method's return type must be {@code java.util.Optional}{@code <T>},
     * or if false, it must be {@link java.util.NavigableSet}{@code <T>}, where {@code T} is some super-type of the type(s)
     * at the remote end of the reference path.
     *
     * @return whether to return only the first object
     */
    boolean firstOnly() default false;
}

