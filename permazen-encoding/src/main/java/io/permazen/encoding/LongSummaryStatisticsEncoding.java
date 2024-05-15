
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.encoding;

import java.util.LongSummaryStatistics;

/**
 * Non-null {@link LongSummaryStatistics} type.
 *
 * <p>
 * Null values are not supported by this class and there is no default value.
 *
 * <p>
 * Instances are ordered by sum, count, min, max.
 */
public class LongSummaryStatisticsEncoding extends Concat4Encoding<LongSummaryStatistics, Long, Long, Long, Long> {

    private static final long serialVersionUID = -1637830934776137662L;

    public LongSummaryStatisticsEncoding() {
        super(LongSummaryStatistics.class,
          new LongEncoding(null),
          new LongEncoding(null),
          new LongEncoding(null),
          new LongEncoding(null),
          LongSummaryStatistics::getSum,
          LongSummaryStatistics::getCount,
          LongSummaryStatistics::getMin,
          LongSummaryStatistics::getMax,
          tuple -> new LongSummaryStatistics(tuple.getValue2(), tuple.getValue3(), tuple.getValue4(), tuple.getValue1()));
    }
}
