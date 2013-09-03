
/*
 * Copyright (C) 2011 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.dellroad.stuff.spring;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.InitializingBean;

/**
 * Generic support superclass for Spring beans.
 */
public abstract class AbstractBean implements InitializingBean, DisposableBean {

    protected final Logger log = LoggerFactory.getLogger(this.getClass());

    /**
     * Initialize bean.
     *
     * <p>
     * The implementation in {@link AbstractBean} does nothing.
     * </p>
     */
    @Override
    public void afterPropertiesSet() throws Exception {
    }

    /**
     * Shutdown bean.
     *
     * <p>
     * The implementation in {@link AbstractBean} does nothing.
     * </p>
     */
    @Override
    public void destroy() throws Exception {
    }
}

