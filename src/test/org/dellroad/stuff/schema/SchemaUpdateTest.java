
/*
 * Copyright (C) 2011 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.dellroad.stuff.schema;

import java.lang.reflect.Method;
import java.util.List;
import java.util.Set;

import javax.sql.DataSource;

import org.dellroad.stuff.TestSupport;
import org.jmock.Expectations;
import org.jmock.Mockery;
import org.springframework.beans.factory.NoSuchBeanDefinitionException;
import org.springframework.context.support.ClassPathXmlApplicationContext;
import org.springframework.util.StringUtils;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class SchemaUpdateTest extends TestSupport {

    protected ClassPathXmlApplicationContext context;

    @Test
    public void testBasicSchemaUpdate() throws Exception {
        this.testSchemaUpdate();
    }

    @Test
    public void testMultiActionSchemaUpdate() throws Exception {
        this.testSchemaUpdate();
    }

    @Test
    public void testUnexpectedSchemaUpdate() throws Exception {
        this.testSchemaUpdate();
    }

    @Test
    public void testInitializeSchemaUpdate() throws Exception {
        this.testSchemaUpdate();
    }

    @Test
    public void testEmptySchemaUpdate() throws Exception {
        this.testSchemaUpdate();
    }

    @BeforeMethod
    public void openContext(Method method) {
        assert this.context == null;
        String filename = StringUtils.uncapitalize(method.getName().substring(4));
        this.context = new ClassPathXmlApplicationContext(filename + ".xml", this.getClass());
    }

    @AfterMethod(alwaysRun = true)
    public void closeContext(Method method) {
        if (this.context != null) {
            this.context.close();
            this.context = null;
        }
    }

    @SuppressWarnings("unchecked")
    protected void testSchemaUpdate() throws Exception {

        // Get test previous and applied updates
        Set<String> previousUpdates = (Set<String>)this.context.getBean("previousUpdates", Set.class);
        List<String> recordedUpdates;
        try {
            recordedUpdates = (List<String>)this.context.getBean("recordedUpdates", List.class);
        } catch (NoSuchBeanDefinitionException e) {
            recordedUpdates = null;
        }

        // Get updater
        TestSchemaUpdater updater = this.context.getBean(TestSchemaUpdater.class);
        updater.setPreviousUpdates(previousUpdates);

        // Perform updates
        Exception exception = null;
        try {
            updater.initializeAndUpdateDatabase(this.mockDataSource());
        } catch (Exception e) {
            exception = e;
        }

        // Verify expected updates were applied
        if (recordedUpdates == null)
            assert exception != null : "expected an exception but didn't get one";
        else {
            if (exception != null)
                throw exception;
            Assert.assertEquals((Object)updater.getUpdatesRecorded(), (Object)recordedUpdates);
        }

        // Verify database initialization
        updater.checkInitialization();
    }

    protected DataSource mockDataSource() {
        Mockery mockery = new Mockery();
        final DataSource dataSource = mockery.mock(DataSource.class);
        mockery.checking(new Expectations() { {
            ignoring(dataSource);
        } });
        return dataSource;
    }
}

