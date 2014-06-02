
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.gui;

import com.google.common.base.Function;
import com.google.common.collect.Lists;
import com.google.common.reflect.TypeToken;
import com.vaadin.data.Container;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import org.dellroad.stuff.java.ErrorAction;
import org.dellroad.stuff.vaadin7.ProvidesProperty;
import org.dellroad.stuff.vaadin7.SimpleKeyedContainer;
import org.dellroad.stuff.vaadin7.VaadinConfigurable;
import org.jsimpledb.JClass;
import org.jsimpledb.JSimpleDB;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;

@SuppressWarnings("serial")
@VaadinConfigurable(ifSessionNotLocked = ErrorAction.EXCEPTION)
public class JClassContainer extends SimpleKeyedContainer<Integer, JClassContainer.Node> implements Container.Hierarchical {

    protected final Logger log = LoggerFactory.getLogger(this.getClass());

    private final ArrayList<Node> rootList = new ArrayList<>();

    @Autowired
    private JSimpleDB jdb;

    /**
     * Constructor.
     */
    public JClassContainer() {
        super(Node.class);
    }

    @Override
    public void connect() {
        super.connect();
        this.reload();
    }

    @Override
    public Integer getKeyFor(Node node) {
        return node.getStorageId();
    }

    public void reload() {

        // Create Node's
        final ArrayList<Node> nodes = new ArrayList<>();
        for (JClass<?> jclass : this.jdb.getJClassesByStorageId().values())
            nodes.add(new Node(jclass));

        // Determine parents by inspecting Java types
        for (Node node1 : nodes) {
            final TypeToken<?> type1 = node1.getJClass().getTypeToken();
            for (Node node2 : nodes) {
                if (node2 == node1)
                    continue;
                final TypeToken<?> type2 = node2.getJClass().getTypeToken();
                if (type2.isAssignableFrom(type1)) {
                    final Node previous = node1.getParent();
                    if (previous == null || previous.getJClass().getTypeToken().isAssignableFrom(type2))
                        node1.setParent(node2);
                }
            }
        }

        // Derive children
        for (Node node1 : nodes) {
            for (Node node2 : nodes) {
                if (node2 == node1.getParent())
                    node2.getChilds().add(node1);
            }
        }

        // Derive roots
        this.rootList.clear();
        for (Node node : nodes) {
            if (node.getParent() == null)
                this.rootList.add(node);
        }

        // Load container
        this.load(nodes);
    }

// Node

    public static class Node {

        public static final Function<Node, Integer> STORAGE_ID_FUNCTION = new Function<Node, Integer>() {
            @Override
            public Integer apply(Node node) {
                return node.getStorageId();
            }
        };

        private final JClass<?> jclass;

        private final ArrayList<Node> childs = new ArrayList<>();
        private Node parent;

        public Node(JClass<?> jclass) {
            this.jclass = jclass;
        }

        public JClass<?> getJClass() {
            return this.jclass;
        }

        public Node getParent() {
            return this.parent;
        }
        public void setParent(Node parent) {
            this.parent = parent;
        }

        public List<Node> getChilds() {
            return this.childs;
        }

        @ProvidesProperty
        public String getName() {
            return this.jclass.getName();
        }

        @ProvidesProperty
        public int getStorageId() {
            return this.jclass.getStorageId();
        }

        @ProvidesProperty
        public String getType() {
            return this.jclass.getTypeToken().toString();
        }
    }

// Container.Hierarchical methods

    @Override
    public Collection<Integer> getChildren(Object itemId) {
        final Node node = this.getJavaObject(itemId);
        return node != null ? Lists.transform(node.getChilds(), Node.STORAGE_ID_FUNCTION) : Collections.<Integer>emptySet();
    }

    @Override
    public Integer getParent(Object itemId) {
        final Node child = this.getJavaObject(itemId);
        if (child == null)
            return null;
        final Node parent = child.getParent();
        if (parent == null)
            return null;
        return parent.getStorageId();
    }

    @Override
    public Collection<Integer> rootItemIds() {
        return Lists.transform(this.rootList, Node.STORAGE_ID_FUNCTION);
    }

    @Override
    public boolean areChildrenAllowed(Object itemId) {
        final Node node = this.getJavaObject(itemId);
        return node != null && !node.getChilds().isEmpty();
    }

    @Override
    public boolean isRoot(Object itemId) {
        final Node node = this.getJavaObject(itemId);
        return node != null && node.getParent() == null;
    }

    @Override
    public boolean hasChildren(Object itemId) {
        final Node node = this.getJavaObject(itemId);
        return node != null && !node.getChilds().isEmpty();
    }

// Mutators are not supported

    @Override
    public boolean setParent(Object itemId, Object newParentId) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean setChildrenAllowed(Object itemId, boolean areChildrenAllowed) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean removeItem(Object itemId) {
        throw new UnsupportedOperationException();
    }
}

