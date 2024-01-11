
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.vaadin;

import com.google.common.base.Preconditions;
import com.vaadin.data.Container;
import com.vaadin.shared.ui.label.ContentMode;

import io.permazen.PermazenClass;
import io.permazen.Permazen;
import io.permazen.Util;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.dellroad.stuff.vaadin7.ProvidesProperty;
import org.dellroad.stuff.vaadin7.SimpleKeyedContainer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Hierarical container that contains the tree of all Java model classes in the database schema.
 */
@SuppressWarnings("serial")
public class TypeContainer extends SimpleKeyedContainer<Class<?>, TypeContainer.Node> implements Container.Hierarchical {

    public static final String NAME_PROPERTY = "name";
    public static final String STORAGE_ID_PROPERTY = "storageId";
    public static final String TYPE_PROPERTY = "type";

    protected final Logger log = LoggerFactory.getLogger(this.getClass());

    private final Permazen jdb;
    private final Class<?> rootType;
    private final ArrayList<Node> rootList = new ArrayList<>();

    /**
     * Constructor.
     *
     * @param jdb underlying database
     */
    public TypeContainer(Permazen jdb) {
        this(jdb, null);
    }

    /**
     * Constructor.
     *
     * @param jdb underlying database
     * @param type type restrictions, or null for none
     */
    public TypeContainer(Permazen jdb, Class<?> type) {
        super(Node.class);
        Preconditions.checkArgument(jdb != null, "null jdb");
        this.jdb = jdb;

        // Get the types of all JClasses assignable to the given type, and use lowest common ancestor as the "top type"
        final HashSet<Class<?>> types = this.jdb.getPermazenClasses().values().stream()
          .filter(jclass -> type == null || type.isAssignableFrom(jclass.getType()))
          .map(PermazenClass::getType)
          .collect(Collectors.toCollection(HashSet::new));
        this.rootType = !types.isEmpty() ? Util.findLowestCommonAncestorOfClasses(types).getRawType() : Object.class;
    }

    @Override
    public void connect() {
        super.connect();
        this.reload();
    }

    @Override
    public Class<?> getKeyFor(Node node) {
        return node.getType();
    }

    public Class<?> getRootType() {
        return this.rootType;
    }

    public void reload() {

        // Node set
        final ArrayList<Node> nodes = new ArrayList<>();

        // Create Node's for each PermazenClass
        boolean addedRoot = false;
        for (PermazenClass<?> jclass : this.jdb.getPermazenClasses().values()) {
            if (this.rootType.isAssignableFrom(jclass.getType())) {
                nodes.add(new Node(jclass));
                if (jclass.getType() == this.rootType)
                    addedRoot = true;
            }
        }

        // Ensure there is a root node
        if (!addedRoot)
            nodes.add(new Node(this.rootType));

        // Sort by name
        Collections.sort(nodes, Comparator.comparing(Node::propertyName));

        // Determine parents by inspecting Java types
        for (Node node1 : nodes) {
            final Class<?> type1 = node1.getType();
            for (Node node2 : nodes) {
                if (node2 == node1)
                    continue;
                final Class<?> type2 = node2.getType();
                if (type2.isAssignableFrom(type1)) {
                    final Node previous = node1.getParent();
                    if (previous == null || previous.getType().isAssignableFrom(type2))
                        node1.setParent(node2);
                }
            }
        }

        // Associate children with parents and identify roots
        this.rootList.clear();
        for (Node node : nodes) {
            if (node.getParent() != null)
                node.getParent().getChilds().add(node);
            else
              this.rootList.add(node);
        }

        // Load container
        this.load(nodes);
    }

// Node

    public static class Node {

        private final Class<?> type;
        private final PermazenClass<?> jclass;
        private final ArrayList<Node> childs = new ArrayList<>();

        private Node parent;

        public Node(PermazenClass<?> jclass) {
            this(jclass.getType(), jclass);
        }

        public Node(Class<?> type) {
            this(type, null);
        }

        private Node(Class<?> type, PermazenClass<?> jclass) {
            Preconditions.checkArgument(type != null, "null type");
            this.type = type;
            this.jclass = jclass;
        }

        public Class<?> getType() {
            return this.type;
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

        @ProvidesProperty(TypeContainer.NAME_PROPERTY)
        public String propertyName() {
            return this.jclass != null ? this.jclass.getName() : this.type.getSimpleName();
        }

        @ProvidesProperty(TypeContainer.STORAGE_ID_PROPERTY)
        public Integer propertyStorageId() {
            return this.jclass != null ? this.jclass.getStorageId() : null;
        }

        @ProvidesProperty(TypeContainer.TYPE_PROPERTY)
        public SizedLabel proeprtyType() {
            return new SizedLabel("<code>" + this.getType().toString() + "</code>", ContentMode.HTML);
        }
    }

// Container.Hierarchical methods

    @Override
    public Set<Class<?>> getChildren(Object itemId) {
        final Node node = this.getJavaObject(itemId);
        return node != null ?
          node.getChilds().stream().map(Node::getType).collect(Collectors.toSet()) :
          Collections.<Class<?>>emptySet();
    }

    @Override
    public Class<?> getParent(Object itemId) {
        final Node child = this.getJavaObject(itemId);
        if (child == null)
            return null;
        final Node parent = child.getParent();
        if (parent == null)
            return null;
        return parent.getType();
    }

    @Override
    public Set<Class<?>> rootItemIds() {
        return this.rootList.stream()
          .map(Node::getType)
          .collect(Collectors.toSet());
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
