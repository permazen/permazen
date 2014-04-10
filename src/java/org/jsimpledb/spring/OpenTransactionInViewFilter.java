
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.spring;

import java.io.IOException;

import javax.servlet.FilterChain;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.jsimpledb.JLayer;
import org.jsimpledb.JTransaction;
import org.jsimpledb.ValidationMode;
import org.springframework.transaction.interceptor.DefaultTransactionAttribute;
import org.springframework.transaction.interceptor.TransactionAttribute;
import org.springframework.web.context.WebApplicationContext;
import org.springframework.web.context.support.WebApplicationContextUtils;
import org.springframework.web.filter.OncePerRequestFilter;

/**
 * A servlet {@link javax.servlet.Filter} binds a {@link JLayer} {@link JTransaction} to the current thread
 * for the entire processing of the request. Intended for the "Open Session in View" pattern, i.e. to allow
 * for lazy loading in web views despite the original transactions already being completed.
 *
 * <p>
 * This filter makes {@link JTransaction}s available via the current thread, which will be autodetected by
 * a {@link JLayerTransactionManager}.
 * </p>
 *
 * <p>
 * Looks up the {@link JLayer} in Spring's root web application context. Supports a {@code "jlayerBeanName"}
 * filter init-param in web.xml; the default bean name is {@code "jlayer"}. Also supports setting the following
 * filter init-params:
 * <ul>
 *  <li>{@code "transactionAttributes"} - configures {@link TransactionAttribute}s; value should be
 *      a string compatible with {@link org.springframework.transaction.interceptor.TransactionAttributeEditor}.</li>
 *  <li>{@code "allowNewSchema"} - whether creation of a new schema version in the database is allowed.
 *      (see {@link JLayer#createTransaction JLayer.createTransaction()}). Default false.</li>
 *  <li>{@code "validationMode"} - validation mode for the transaction
 *      (see {@link JLayer#createTransaction JLayer.createTransaction()}). Default {@link ValidationMode#AUTOMATIC}.</li>
 * </ul>
 * </p>
 */
public class OpenTransactionInViewFilter extends OncePerRequestFilter {

    /**
     * The default name of the {@link JLayer} bean: <code>{@value}</code>.
     *
     * @see #JLAYER_BEAN_NAME_PARAMETER
     */
    public static final String DEFAULT_JLAYER_BEAN_NAME = "jlayer";

    /**
     * Filter init parameter that specifies the name of the {@link JLayer} bean: <code>{@value}</code>.
     *
     * @see #DEFAULT_JLAYER_BEAN_NAME
     */
    public static final String JLAYER_BEAN_NAME_PARAMETER = "jlayerBeanName";

    /**
     * Filter init parameter that specifies transaction attributes: <code>{@value}</code>.
     */
    public static final String JLAYER_TRANSACTION_ATTRIBUTE_PARAMETER = "transactionAttributes";

    /**
     * Filter init parameter that specifies whether creation of a new schema version in the database is allowed.
     * Default false.
     */
    public static final String JLAYER_ALLOW_NEW_SCHEMA_PARAMETER = "allowNewSchema";

    /**
     * Filter init parameter that specifies the validation mode for the transaction.
     * Default {@link ValidationMode#AUTOMATIC}.
     */
    public static final String JLAYER_VALIDATION_MODE_PARAMETER = "validationMode";

    private String jlayerBeanName = DEFAULT_JLAYER_BEAN_NAME;
    private TransactionAttribute transactionAttributes;
    private boolean allowNewSchema;
    private ValidationMode validationMode = ValidationMode.AUTOMATIC;

    private volatile JLayer jlayer;

    /**
     * Get the name of the {@link JLayer} bean to find in the Spring root application context.
     *
     * @see #JLAYER_BEAN_NAME_PARAMETER
     */
    public String getJlayerBeanName() {
        return this.jlayerBeanName;
    }

    /**
     * Set the name of the {@link JLayer} bean to find in the Spring root application context.
     * Default is {@link #DEFAULT_JLAYER_BEAN_NAME}.
     *
     * @see #JLAYER_BEAN_NAME_PARAMETER
     */
    public void setJlayerBeanName(String jlayerBeanName) {
        this.jlayerBeanName = jlayerBeanName;
    }

    /**
     * Get the transaction attributes.
     *
     * @see #JLAYER_TRANSACTION_ATTRIBUTE_PARAMETER
     */
    public TransactionAttribute getTransactionAttributes() {
        return this.transactionAttributes;
    }

    /**
     * Set the transaction attributes.
     *
     * @see #JLAYER_TRANSACTION_ATTRIBUTE_PARAMETER
     */
    public void setTransactionAttributes(TransactionAttribute transactionAttributes) {
        this.transactionAttributes = transactionAttributes;
    }

    /**
     * Get whether new scheme creation is allowed.
     *
     * @see #JLAYER_ALLOW_NEW_SCHEMA_PARAMETER
     */
    public boolean isAllowNewSchema() {
        return this.allowNewSchema;
    }

    /**
     * Set whether new scheme creation is allowed.
     *
     * @see #JLAYER_ALLOW_NEW_SCHEMA_PARAMETER
     */
    public void setAllowNewSchema(boolean allowNewSchema) {
        this.allowNewSchema = allowNewSchema;
    }

    /**
     * Get transaction validation mode.
     *
     * @see #JLAYER_VALIDATION_MODE_PARAMETER
     */
    public ValidationMode getValidationMode() {
        return this.validationMode;
    }

    /**
     * Set transaction validation mode.
     *
     * @see #JLAYER_VALIDATION_MODE_PARAMETER
     */
    public void setValidationMode(ValidationMode validationMode) {
        if (validationMode == null)
            validationMode = ValidationMode.AUTOMATIC;
        this.validationMode = validationMode;
    }

    /**
     * Look up the {@link JLayer} that this filter should use.
     *
     * @return the JLayer to use
     * @see #getJlayerBeanName
     */
    protected JLayer lookupJLayer() {
        if (this.jlayer == null) {
            final WebApplicationContext wac = WebApplicationContextUtils.getRequiredWebApplicationContext(this.getServletContext());
            this.jlayer = wac.getBean(this.getJlayerBeanName(), JLayer.class);
        }
        return this.jlayer;
    }

    @Override
    protected void doFilterInternal(final HttpServletRequest request, final HttpServletResponse response,
      final FilterChain filterChain) throws ServletException, IOException {

        // Sanity check
        try {
            JTransaction.getCurrent();
            throw new IllegalStateException("a JTransaction is already associated with the current thread");
        } catch (IllegalStateException e) {
            // expected
        }

        // Get transaction attributes
        TransactionAttribute attr = this.transactionAttributes;
        if (attr == null)
            attr = new DefaultTransactionAttribute();

        // Create transaction
        final JTransaction jtx = this.lookupJLayer().createTransaction(this.allowNewSchema, this.validationMode);

        // Configure it
        if (attr.isReadOnly())
            jtx.getTransaction().setReadOnly(true);
        final int timeout = attr.getTimeout();
        if (timeout != TransactionAttribute.TIMEOUT_DEFAULT) {
            try {
                jtx.getTransaction().setTimeout(timeout * 1000L);
            } catch (UnsupportedOperationException e) {
                // ignore
            }
        }

        // Proceed with transaction
        boolean success = false;
        try {
            JTransaction.setCurrent(jtx);
            filterChain.doFilter(request, response);
            success = true;
        } catch (Throwable t) {
            if (attr.rollbackOn(t) || jtx.getTransaction().isRollbackOnly())
                jtx.rollback();
            else
                jtx.commit();
            if (t instanceof Error)
                throw (Error)t;
            if (t instanceof RuntimeException)
                throw (Error)t;
            if (t instanceof ServletException)
                throw (ServletException)t;
            if (t instanceof IOException)
                throw (IOException)t;
            throw new RuntimeException(t);
        } finally {
            JTransaction.setCurrent(null);
            if (success) {
                if (jtx.getTransaction().isRollbackOnly())
                    jtx.rollback();
                else
                    jtx.commit();
            }
        }
    }
}

