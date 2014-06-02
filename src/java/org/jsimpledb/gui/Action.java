
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.gui;

import org.dellroad.stuff.spring.RetryTransaction;
import org.springframework.transaction.annotation.Transactional;

public abstract class Action extends com.vaadin.event.Action {

    protected final boolean transactional;

    public Action(String caption) {
        this(caption, false);
    }

    public Action(String caption, boolean transactional) {
        super(caption);
        this.transactional = transactional;
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
        if (this.transactional)
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

