
/*
 * Copyright (C) 2011 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.dellroad.stuff.spring;

import java.util.Comparator;
import java.util.HashMap;

import org.springframework.beans.factory.ListableBeanFactory;

/**
 * A {@link Comparator} that orders Spring bean names in the same order as the corresponding
 * beans appear in a {@link ListableBeanFactory}.
 *
 * <p>
 * Names that are not present in the configured {@link ListableBeanFactory} cause an exception.
 */
public class BeanNameComparator implements Comparator<String> {

    private final HashMap<String, Integer> beanNameMap;
    private final String factoryName;

    public BeanNameComparator(ListableBeanFactory beanFactory) {
        String[] beanNames = beanFactory.getBeanDefinitionNames();
        this.beanNameMap = new HashMap<String, Integer>(beanNames.length);
        for (int i = 0; i < beanNames.length; i++)
            this.beanNameMap.put(beanNames[i], i);
        this.factoryName = "" + beanFactory;
    }

    @Override
    public int compare(String name1, String name2) {
        Integer index1 = this.beanNameMap.get(name1);
        Integer index2 = this.beanNameMap.get(name2);
        if (index1 == null)
            throw new IllegalArgumentException("failed to find bean `" + name1 + "' in bean factory " + this.factoryName);
        if (index2 == null)
            throw new IllegalArgumentException("failed to find bean `" + name2 + "' in bean factory " + this.factoryName);
        return index1 - index2;
    }
}

