
/*
 * Copyright (C) 2011 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.dellroad.stuff.xml;

import javax.xml.transform.ErrorListener;
import javax.xml.transform.TransformerException;

import org.slf4j.Logger;

/**
 * {@link ErrorListener} implementation that logs the messgaes to a configured {@link Logger}
 * and throws exceptions in cases of errors and fatal errors.
 *
 * <p>
 * This class also optionally works around some stupid Xalan-J bugs:
 * <ul>
 * <li>Throw exceptions in the case of {@link #warning} also</li>
 * <li>When throwing exceptions, wrap them in {@link RuntimeException}s to avoid being swallowed</li>
 * </ul>
 */
public class TransformErrorListener implements ErrorListener {

    protected final Logger log;
    protected final boolean xalanWorkarounds;

    public TransformErrorListener(Logger log, boolean xalanWorkarounds) {
        this.log = log;
        this.xalanWorkarounds = xalanWorkarounds;
    }

    @Override
    public void warning(TransformerException e) throws TransformerException {
        this.log.warn(this.getLogMessageFor(e));
        if (this.xalanWorkarounds)
            this.rethrow(e);
    }

    @Override
    public void error(TransformerException e) throws TransformerException {
        this.log.error(this.getLogMessageFor(e));
        this.rethrow(e);
    }

    @Override
    public void fatalError(TransformerException e) throws TransformerException {
        this.log.error(this.getLogMessageFor(e));
        this.rethrow(e);
    }

    protected String getLogMessageFor(TransformerException e) {
        return e.getMessageAndLocation();
    }

    protected void rethrow(TransformerException e) throws TransformerException {
        if (!this.xalanWorkarounds)
            throw e;
        throw new RuntimeException("exception from XSL transform", e);
    }
}

