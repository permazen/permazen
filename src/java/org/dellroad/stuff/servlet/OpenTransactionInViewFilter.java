
/*
 * Copyright (C) 2011 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.dellroad.stuff.servlet;

import java.io.IOException;

import javax.servlet.FilterChain;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionStatus;
import org.springframework.transaction.support.DefaultTransactionDefinition;
import org.springframework.transaction.support.TransactionCallback;
import org.springframework.transaction.support.TransactionTemplate;
import org.springframework.web.context.WebApplicationContext;
import org.springframework.web.context.support.WebApplicationContextUtils;
import org.springframework.web.filter.OncePerRequestFilter;

/**
 * Servlet filter that wraps execution in a transaction. A {@link TransactionTemplate} must exist in the
 * associated {@link WebApplicationContext} and is identified by name via {@link #setTransactionManagerBeanName
 * setTransactionManagerBeanName()} (by default, {@link #DEFAULT_TRANSACTION_MANAGER_BEAN_NAME}).
 *
 * <p>
 * Transaction properties are configurable via filter <code>&lt;init-param&gt;</code>'s {@code isolation},
 * {@code propagation}, and {@code readOnly}.
 * </p>
 */
public class OpenTransactionInViewFilter extends OncePerRequestFilter {

    public static final String DEFAULT_TRANSACTION_MANAGER_BEAN_NAME = "transactionManager";

    protected final Logger log = LoggerFactory.getLogger(getClass());

    private final DefaultTransactionDefinition transactionDefinition = new DefaultTransactionDefinition();

    private WebApplicationContext webApplicationContext;
    private String transactionManagerBeanName = DEFAULT_TRANSACTION_MANAGER_BEAN_NAME;
    private PlatformTransactionManager transactionManager;

    @Override
    protected void initFilterBean() throws ServletException {
        super.initFilterBean();
        log.debug("finding containing WebApplicationContext");
        try {
            this.webApplicationContext = WebApplicationContextUtils.getRequiredWebApplicationContext(getServletContext());
        } catch (IllegalStateException e) {
            throw new ServletException("could not locate containing WebApplicationContext");
        }
    }

    public void setIsolation(String isolation) {
        this.transactionDefinition.setIsolationLevelName(isolation);
    }

    public void setPropagation(String propagation) {
        this.transactionDefinition.setPropagationBehaviorName(propagation);
    }

    public void setReadOnly(boolean readOnly) {
        this.transactionDefinition.setReadOnly(readOnly);
    }

    public void setTransactionManagerBeanName(String transactionManagerBeanName) {
        this.transactionManagerBeanName = transactionManagerBeanName;
    }

    protected synchronized PlatformTransactionManager getTransactionManager() {
        if (this.transactionManager == null) {
            this.transactionManager = this.webApplicationContext.getBean(
              this.transactionManagerBeanName, PlatformTransactionManager.class);
        }
        return this.transactionManager;
    }

    @Override
    protected void doFilterInternal(final HttpServletRequest request, final HttpServletResponse response,
      final FilterChain filterChain) throws ServletException, IOException {
        TransactionTemplate transactionTemplate = new TransactionTemplate(getTransactionManager(), this.transactionDefinition);
        try {
            transactionTemplate.execute(new TransactionCallback<Void>() {

                @Override
                public Void doInTransaction(TransactionStatus status) {
                    try {
                        filterChain.doFilter(request, response);
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    } catch (ServletException e) {
                        throw new RuntimeException(e);
                    }
                    return null;
                }
            });
        } catch (RuntimeException e) {
            Throwable nested = e.getCause();
            if (nested instanceof IOException)
                throw (IOException)nested;
            if (nested instanceof ServletException)
                throw (ServletException)nested;
            throw e;
        }
    }
}

