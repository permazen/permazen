
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.annotation;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.util.NavigableSet;
import java.util.Optional;

/**
 * Annotates Java methods that should return all objects found at the far end of a {@link io.permazen.ReferencePath}.
 *
 * <p>
 * This annotation can be used as a convenience to let Permazen auto-generate reference path traversal code.
 * A common use case is inverting references, e.g., from a parent back to a child in a one-to-many relationship.
 *
 * <p>
 * Annotating an abstract method with {@code @ReferencePath("->some->path")} is equivalent to providing an implementation
 * that returns all objects reachable via the {@linkplain io.permazen.ReferencePath reference path} {@code "->some->path"} when
 * starting at the current instance. References can be traversed in either the forward or inverse directions; see
 * {@link io.permazen.ReferencePath} for details on reference paths.
 *
 * <p>
 * For example:
 * <pre>
 *  &#64;PermazenType
 *  public interface TreeNode extends PermazenObject {
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
 *      &#64;ReferencePath("&lt;-TreeNode.parent")
 *      NavigableSet&lt;TreeNode&gt; getChildren();
 *
 *      /**
 *       * Get this node's grandparent, if any.
 *       *&#47;
 *      &#64;ReferencePath("-&gt;parent-&gt;parent")
 *      Optional&lt;TreeNode&gt; getGrandparent();
 *  }
 * </pre>
 * In the example above, the generated {@code getChildren()} implementation will be functionally equivalent to
 * (and slightly more efficient than) this one:
 * <pre>
 *      /**
 *       * Get the children of this node.
 *       *&#47;
 *      NavigableSet&lt;TreeNode&gt; getChildren() {
 *          final PermazenTransaction jtx = this.getTransaction();
 *          final ReferencePath path = jtx.getPermazen().parseReferencePath(this.getClass(), "&lt;-TreeNode.parent");
 *          return jtx.followReferencePath(path, Collections.singleton(this));
 *      }
 *  }
 * </pre>
 *
 * <p><b>Method Return Type</b></p>
 *
 * <p>
 * The annotated method must be an instance method and return either {@link NavigableSet NavigableSet&lt;T&gt;}
 * or {@link Optional}{@code <T>}, where {@code T} is any super-type of the target object type(s) at the end
 * of the reference path.
 *
 * <p>
 * If the reference path contains only forward traversals of many-to-one relationships (i.e., simple reference fields),
 * then only one object can be returned and the return type must be {@link Optional}{@code <T>}.
 *
 * <p>
 * Otherwise, either return type is valid, and you can use {@link Optional}{@code <T>} if you only care to
 * retrieve at most a single instance.
 *
 * <p>
 * More examples:
 * <pre>
 *  &#64;PermazenType
 *  public interface TreeNode extends PermazenObject {
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
 *      <b>&#64;ReferencePath("&lt;-TreeNode.parent")</b>
 *      NavigableSet&lt;TreeNode&gt; getChildren();
 *
 *      /**
 *       * Get the siblings of this node (including this node).
 *       *&#47;
 *      <b>&#64;ReferencePath("-&gt;parent&lt;-TreeNode.parent")</b>
 *      NavigableSet&lt;TreeNode&gt; getSiblings();
 *
 *      /**
 *       * Get second cousins once removed (and parents), but only the first.
 *       *&#47;
 *      <b>&#64;ReferencePath("-&gt;parent-&gt;parent-&gt;parent&lt;-TreeNode.parent&lt;-TreeNode.parent")</b>
 *      Optional&lt;TreeNode&gt; getSecondCousinOnceRemoved();
 *  }
 * </pre>
 *
 * <p><b>Meta-Annotations</b></p>
 *
 * <p>
 * This annotation may be configured indirectly as a Spring
 * <a href="https://github.com/spring-projects/spring-framework/wiki/Spring-Annotation-Programming-Model#meta-annotations">meta-annotation</a>
 * when {@code spring-core} is on the classpath.
 */
@Retention(RetentionPolicy.RUNTIME)
@Target({ ElementType.ANNOTATION_TYPE, ElementType.METHOD })
@Documented
public @interface ReferencePath {

    /**
     * The reference path to follow.
     *
     * <p>
     * The starting type for the reference path is the class containing the annotated method.
     *
     * @return the reference path to traverse
     * @see io.permazen.ReferencePath
     */
    String value() default "";
}
