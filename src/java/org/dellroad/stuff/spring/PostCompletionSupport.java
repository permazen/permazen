
/*
 * Copyright (C) 2011 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.dellroad.stuff.spring;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Inherited;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Method-level annotation that allows threads executing within the annotated method to request the execution of
 * post-completion callbacks.
 *
 * <p>
 * This annotation is useful in the following situation: you want to perform some action asynchronously,
 * but you want to delay the execution of the asynchronous action until the current transaction commits,
 * so that the asynchronous action can see the updated database state; however, (in most cases) if the
 * current transaction fails, you don't want to execute the asynchronous action at all.
 *
 * <p>
 * To do this, put a {@link PostCompletionSupport @PostCompletionSupport} annotation everywhere you have
 * a {@code @Transactional} annotation, and then use the {@link PostCompletion#execute PostCompletion.execute(Runnable)}
 * variants to register post-completion actions at any time during the execution of the annotated methods:
 * <blockquote><pre>
 *  <b>&#64;PostCompletionSupport</b>
 *  &#64;Transactional
 *  public void doSomething() {
 *
 *      // Do whatever...
 *
 *      <b>PostCompletion.execute</b>(new Runnable() {
 *          public void run() {
 *              // This executes only after doSomething() has successfully completed
 *              //  and the transaction around doSomething() has closed
 *          }
 *      });
 *
 *      // Do whatever else...
 *  }
 * </pre></blockquote>
 *
 * <p>
 * To activate this annotation, you must run the AspectJ compiler to apply the dellroad-stuff aspect that
 * detects {@link PostCompletionSupport @PostCompletionSupport} annotations, and specify the executor to
 * use in your application context. For example:
 * <blockquote><pre>
 *  &lt;!-- enable post-completion support --&gt;
 *  &lt;dellroad-stuff:post-completion executor="myExecutor"/&gt;
 *
 *  &lt;!-- post-completion requires an Executor --&gt;
 *  &lt;task:executor id="myExecutor" pool-size="5"/&gt;
 * </pre></blockquote>
 * </p>
 *
 * @see PostCompletion
 */
@Target({ ElementType.METHOD })
@Retention(RetentionPolicy.RUNTIME)
@Inherited
@Documented
public @interface PostCompletionSupport {
}

