
/*
 * Copyright (C) 2011 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.dellroad.stuff.vaadin7;

import org.springframework.web.context.ConfigurableWebApplicationContext;

/**
 * Vaadin 7 version of {@link org.dellroad.stuff.vaadin.SpringContextApplication}.
 *
 * @see org.dellroad.stuff.vaadin.SpringContextApplication
 */
@SuppressWarnings("serial")
public class SpringContextApplication extends org.dellroad.stuff.vaadin.SpringContextApplication {

    /**
     * Initialize the application. In Vaadin 7 overriding this method is optional.
     *
     * <p>
     * The implementation in {@link ContextApplication} does nothing.
     */
    @Override
    protected void initSpringApplication(ConfigurableWebApplicationContext context) {
    }
}

