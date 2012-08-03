
/*
 * Copyright (C) 2011 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.dellroad.stuff.pobj;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.lang.reflect.Method;
import java.net.URL;

import org.dellroad.stuff.TestSupport;
import org.jibx.extras.DocumentComparator;
import org.springframework.context.support.ClassPathXmlApplicationContext;
import org.springframework.util.StringUtils;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class PersistentObjectTest extends TestSupport {

    protected ClassPathXmlApplicationContext context;

    private String testName;
    private File actualFile;
    private File expectedFile;

    @BeforeMethod
    public void openContext(Method method) throws Exception {
        assert this.context == null;

        // Get test name
        this.testName = StringUtils.uncapitalize(method.getName().substring(4));

        // Get before and after files
        URL url = this.getClass().getResource(this.getClass().getSimpleName() + ".class");
        String dir = url.toString();
        dir = dir.substring(dir.indexOf(':') + 1, dir.lastIndexOf('/'));
        this.actualFile = new File(dir, this.testName + ".pobj.xml");
        this.expectedFile = new File(dir, this.testName + ".out.xml");

        // Open application context
        boolean expectError = this.testName.contains("error");
        try {
            this.context = new ClassPathXmlApplicationContext(this.testName + ".xml", this.getClass());
            assert !expectError : "expected error but didn't get one";
        } catch (Exception e) {
            if (!expectError)
                throw e;
        }
    }

    @AfterMethod(alwaysRun = true)
    public void closeContext(Method method) {
        if (this.context != null) {
            this.context.close();
            this.context = null;
        }
        this.testName = null;
        this.actualFile = null;
        this.expectedFile = null;
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testBasic() throws Exception {

        // Get schema updater
        PersistentObjectSchemaUpdater<?> updater = this.context.getBean(PersistentObjectSchemaUpdater.class);

        // Get persistent object
        PersistentObject<RootObject> pobj = (PersistentObject<RootObject>)this.context.getBean(PersistentObject.class);
        assert pobj.getFile().exists();
        new File(pobj.getFile() + ".1").delete();
        new File(pobj.getFile() + ".2").delete();

        // Make changes
        RootObject root = pobj.getRoot();
        long version = pobj.getVersion();
        root.setName(root.getName() + ".new");
        root.setVerbose(!root.isVerbose());

        // Verify root was copied when read
        assert pobj.getRoot().isVerbose() != root.isVerbose() : "root not copied when read";

        // Test validation exception
        String temp = root.getName();
        root.setName(null);
        try {
            pobj.setRoot(root, version);
            throw new RuntimeException("expected validation exception");
        } catch (PersistentObjectValidationException e) {
            // expected
        }
        root.setName(temp);

        // Write it back
        pobj.setRoot(root, version);

        // Verify value was copied correctly
        assert pobj.getRoot().equals(root) : "what got set is not what I wrote";

        // Verify root was copied when written
        root.setVerbose(!root.isVerbose());
        assert pobj.getRoot().isVerbose() != root.isVerbose() : "root not copied when written";

        // Verify backup was made
        assert new File(pobj.getFile() + ".1").exists();
        assert !new File(pobj.getFile() + ".2").exists();

        // Test optimistic lock excception
        try {
            pobj.setRoot(root, version);
            throw new RuntimeException("expected lock exception for version " + version + " != " + pobj.getVersion());
        } catch (PersistentObjectVersionException e) {
            // expected
        }

        // Verify value did not get set
        assert !pobj.getRoot().equals(root) : "value changed unexpectedly";

        // Verify result
        InputStreamReader reader1 = new InputStreamReader(new FileInputStream(this.actualFile), "UTF-8");
        InputStreamReader reader2 = new InputStreamReader(new FileInputStream(this.expectedFile), "UTF-8");
        assert new DocumentComparator(System.out, false).compare(reader1, reader2) : "different XML results";
        reader1.close();
        reader2.close();
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testErrorUpdate() throws Exception {
    }
}

