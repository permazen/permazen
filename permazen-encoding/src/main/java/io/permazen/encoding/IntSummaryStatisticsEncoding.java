
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.encoding;

import java.util.IntSummaryStatistics;

/**
 * Non-null {@link IntSummaryStatistics} type.
 *
 * <p>
 * Null values are not supported by this class and there is no default value.
 *
 * <p>
 * Instances are ordered by sum, count, min, max.
 */
public class IntSummaryStatisticsEncoding extends Concat4Encoding<IntSummaryStatistics, Long, Long, Integer, Integer> {

    private static final long serialVersionUID = -1637830934776137662L;

    public IntSummaryStatisticsEncoding() {
        super(IntSummaryStatistics.class,
          new LongEncoding(null),
          new LongEncoding(null),
          new IntegerEncoding(null),
          new IntegerEncoding(null),
          IntSummaryStatistics::getSum,
          IntSummaryStatistics::getCount,
          IntSummaryStatistics::getMin,
          IntSummaryStatistics::getMax,
          tuple -> new IntSummaryStatistics(tuple.getValue2(), tuple.getValue3(), tuple.getValue4(), tuple.getValue1()));
    }
}
