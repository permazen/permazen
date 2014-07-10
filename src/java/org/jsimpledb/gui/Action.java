
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.gui;

import org.dellroad.stuff.spring.RetryTransaction;
import org.springframework.transaction.annotation.Transactional;

/**
 * Extends Vaadin's {@link com.vaadin.event.Action} with an option for transactional operation.
 */
public abstract class Action extends com.vaadin.event.Action {

    protected final boolean transactional;

    /**
     * Convenience constructor. Creates an instance that will not be performed transactionally.
     *
     * @param caption caption for this action
     */
    public Action(String caption) {
        this(caption, false);
    }

    /**
     * Constructor.
     *
     * @param caption caption for this action
     * @param transactional true if action should be performed within a transaction
     */
    public Action(String caption, boolean transactional) {
        super(caption);
        this.transactional = transactional;
    }

    /**
     * Determine if this instance is transactional.
     */
    public boolean isTransactional() {
        return this.transactional;
    }

    /**
     * Perform this action.
     *
     * <p>
     * If this action is transactional, there will already be a transaction open.
     * </p>
     */
    protected abstract void performAction();

    /**
     * Execute this action.
     */
    public final void execute() {
        if (this.isTransactional())
            this.executeTransactionally();
        else
            this.performAction();
    }

    @RetryTransaction
    @Transactional("jsimpledbGuiTransactionManager")
    protected void executeTransactionally() {
        this.performAction();
    }
}

