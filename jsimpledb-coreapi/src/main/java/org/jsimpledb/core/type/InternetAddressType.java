
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb.core.type;

import com.google.common.base.Converter;

import java.io.Serializable;

import javax.mail.internet.AddressException;
import javax.mail.internet.InternetAddress;

/**
 * {@link InternetAddress} email address field type. Use requires {@code javax.mail} on the classpath.
 *
 * <p>
 * Null values are supported by this class.
 *
 * <p>
 * <b>Note:</b> the method {@link InternetAddress#equals InternetAddress.equals()} performs a case-insensitive
 * comparison of email addresses, ignoring the personal name, whereas this field type distinguishes instances
 * that are not exactly equal.
 */
public class InternetAddressType extends StringEncodedType<InternetAddress> {

    private static final long serialVersionUID = 289940859247032224L;

    public InternetAddressType() {
        super(InternetAddress.class, 0, new InternetAddressConverter());
    }

// EmailConverter

    private static class InternetAddressConverter extends Converter<InternetAddress, String> implements Serializable {

        private static final long serialVersionUID = 3837763387234872160L;

        @Override
        protected String doForward(InternetAddress address) {
            if (address == null)
                return null;
            return address.toString();
        }

        @Override
        protected InternetAddress doBackward(String string) {
            if (string == null)
                return null;
            try {
                return new InternetAddress(string);
            } catch (AddressException e) {
                throw new IllegalArgumentException("invalid email address `" + string + "'", e);
            }
        }
    }
}

