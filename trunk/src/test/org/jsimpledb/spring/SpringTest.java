
/*
 * Copyright (C) 2011 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.spring;

import org.jsimpledb.TestSupport;
import org.springframework.context.support.ClassPathXmlApplicationContext;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;

public abstract class SpringTest extends TestSupport {

    protected ClassPathXmlApplicationContext context;

    @BeforeClass
    public void openContext() {
        this.context = new ClassPathXmlApplicationContext(getClass().getSimpleName() + ".xml", getClass());
        this.context.refresh();
    }

    @AfterClass(alwaysRun = true)
    public void closeContext() {
        if (this.context != null) {
            this.context.close();
            this.context = null;
        }
    }
}

