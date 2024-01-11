
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen;

import io.permazen.annotation.PermazenField;

/**
 * JSR 303 validation group for Permazen uniqueness constraints.
 *
 * <p>
 * Explicit use of this class only needed if you wish to manually trigger validation of uniqueness constraints only,
 * for example:
 *  <pre>
 *  user.setUsername(u);
 *  user.revalidate(UniquenessConstraints.class);
 *  user.getTransaction().validate();           // throws exception if 'u' is not unique
 *  </pre>
 *
 * @see PermazenField#unique
 * @see PermazenObject#revalidate PermazenObject.revalidate()
 * @see PermazenTransaction#validate PermazenTransaction.validate()
 */
public interface UniquenessConstraints {
}
