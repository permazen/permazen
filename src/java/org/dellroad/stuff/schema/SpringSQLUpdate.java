
/*
 * Copyright (C) 2011 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.dellroad.stuff.schema;

import org.springframework.beans.factory.BeanFactory;
import org.springframework.beans.factory.BeanFactoryAware;
import org.springframework.beans.factory.BeanNameAware;
import org.springframework.beans.factory.InitializingBean;

/**
 * Provides the functionality of both {@link SQLUpdate} and {@link AbstractSpringSchemaUpdate}.
 */
public class SpringSQLUpdate extends SQLUpdate implements BeanNameAware, BeanFactoryAware, InitializingBean {

    @Override
    public void afterPropertiesSet() throws Exception {
        if (this.sqlScript == null)
            throw new Exception("no SQL script configured");
    }

    @Override
    public void setBeanName(String beanName) {
        this.setName(beanName);
    }

    @Override
    public void setBeanFactory(BeanFactory beanFactory) {
        AbstractSpringSchemaUpdate.setRequiredPredecessorsFromDependencies(this, beanFactory);
    }
}

