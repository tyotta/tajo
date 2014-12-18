/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.tajo.engine.function.datetime;

import org.apache.tajo.catalog.Column;
import org.apache.tajo.common.TajoDataTypes;
import org.apache.tajo.datum.*;
import org.apache.tajo.plan.function.GeneralFunction;
import org.apache.tajo.engine.function.annotation.Description;
import org.apache.tajo.engine.function.annotation.ParamTypes;
import org.apache.tajo.storage.Tuple;
import org.apache.tajo.util.datetime.DateTimeConstants;
import org.apache.tajo.util.datetime.DateTimeUtil;
import org.apache.tajo.util.datetime.TimeMeta;

import static org.apache.tajo.common.TajoDataTypes.Type.*;

@Description(
    functionName = "date_part",
    description = "Extract field from timestamp",
    example = "> SELECT date_part('year', timestamp '2014-01-17 10:09:37.5');\n"
        + "2014.0",
    returnType = TajoDataTypes.Type.FLOAT8,
    paramTypes = {@ParamTypes(paramTypes = {TajoDataTypes.Type.TEXT, TajoDataTypes.Type.TIMESTAMP})}
)
public class DatePartFromTimestamp extends GeneralFunction {
  public DatePartFromTimestamp() {
    super(new Column[] {
        new Column("target", FLOAT8),
        new Column("source", TEXT)
    });
  }

  private DatePartExtractorFromTimestamp extractor = null;

  @Override
  public Datum eval(Tuple params) {
    Datum target = params.get(0);
    TimestampDatum timestamp;

    if(target instanceof NullDatum || params.get(1) instanceof NullDatum) {
      return NullDatum.get();
    }

    if(params.get(1) instanceof TimestampDatum) {
      timestamp = (TimestampDatum)(params.get(1));
    } else {
      return NullDatum.get();
    }

    if (extractor == null) {
      String extractType = target.asChars().toLowerCase();

      if (extractType.equals("century")) {
        extractor = new CenturyExtractorFromTimestamp();
      } else if (extractType.equals("day")) {
        extractor = new DayExtractorFromTimestamp();
      } else if (extractType.equals("decade")) {
        extractor = new DecadeExtractorFromTimestamp();
      } else if (extractType.equals("dow")) {
        extractor = new DowExtractorFromTimestamp();
      } else if (extractType.equals("doy")) {
        extractor = new DoyExtractorFromTimestamp();
      } else if (extractType.equals("epoch")) {
        extractor = new EpochExtractorFromTimestamp();
      } else if (extractType.equals("hour")) {
        extractor = new HourExtractorFromTimestamp();
      } else if (extractType.equals("isodow")) {
        extractor = new ISODowExtractorFromTimestamp();
      } else if (extractType.equals("isoyear")) {
        extractor = new ISOYearExtractorFromTimestamp();
      } else if (extractType.equals("microseconds")) {
        extractor = new MicrosecondsExtractorFromTimestamp();
      } else if (extractType.equals("millennium")) {
        extractor = new MillenniumExtractorFromTimestamp();
      } else if (extractType.equals("milliseconds")) {
        extractor = new MillisecondsExtractorFromTimestamp();
      } else if (extractType.equals("minute")) {
        extractor = new MinuteExtractorFromTimestamp();
      } else if (extractType.equals("month")) {
        extractor = new MonthExtractorFromTimestamp();
      } else if (extractType.equals("quarter")) {
        extractor = new QuarterExtractorFromTimestamp();
      } else if (extractType.equals("second")) {
        extractor = new SecondExtractorFromTimestamp();
      } else if (extractType.equals("timezone")) {
        extractor = new NullExtractorFromTimestamp();
      } else if (extractType.equals("timezone_hour")) {
        extractor = new NullExtractorFromTimestamp();
      } else if (extractType.equals("timezone_minute")) {
        extractor = new NullExtractorFromTimestamp();
      } else if (extractType.equals("week")) {
        extractor = new WeekExtractorFromTimestamp();
      } else if (extractType.equals("year")) {
        extractor = new YearExtractorFromTimestamp();
      } else {
        extractor = new NullExtractorFromTimestamp();
      }
    }

    TimeMeta tm = timestamp.toTimeMeta();
    DateTimeUtil.toUserTimezone(tm);

    return extractor.extract(tm);
  }

  private interface DatePartExtractorFromTimestamp {
    public Datum extract(TimeMeta tm);
  }

  private class CenturyExtractorFromTimestamp implements DatePartExtractorFromTimestamp {
    @Override
    public Datum extract(TimeMeta tm) {
      return DatumFactory.createFloat8((double) tm.getCenturyOfEra());
    }
  } 

  private class DayExtractorFromTimestamp implements DatePartExtractorFromTimestamp {
    @Override
    public Datum extract(TimeMeta tm) {
      return DatumFactory.createFloat8((double) tm.dayOfMonth);
    }
  }

  private class DecadeExtractorFromTimestamp implements DatePartExtractorFromTimestamp {
    @Override
    public Datum extract(TimeMeta tm) {
      return DatumFactory.createFloat8((double) (tm.years / 10));
    }
  }

  private class DowExtractorFromTimestamp implements DatePartExtractorFromTimestamp {
    @Override
    public Datum extract(TimeMeta tm) {
      Integer tdow = tm.getDayOfWeek();
      return DatumFactory.createFloat8((double) ((tdow == 7) ? 0 : tdow));
    }
  }

  private class DoyExtractorFromTimestamp implements DatePartExtractorFromTimestamp {
    @Override
    public Datum extract(TimeMeta tm) {
      return DatumFactory.createFloat8((double)tm.getDayOfYear());
    }
  }

  private class EpochExtractorFromTimestamp implements DatePartExtractorFromTimestamp {
    @Override
    public Datum extract(TimeMeta tm) {
      return DatumFactory.createFloat8((double)DateTimeUtil.julianTimeToEpoch(DateTimeUtil.toJulianTimestamp(tm)));
    }
  }

  private class HourExtractorFromTimestamp implements DatePartExtractorFromTimestamp {
    @Override
    public Datum extract(TimeMeta tm) {
      return DatumFactory.createFloat8((double) tm.hours);
    }
  }

  private class ISODowExtractorFromTimestamp implements DatePartExtractorFromTimestamp {
    @Override
    public Datum extract(TimeMeta tm) {
      return DatumFactory.createFloat8((double) tm.getISODayOfWeek());
    }
  }

  private class ISOYearExtractorFromTimestamp implements DatePartExtractorFromTimestamp {
    @Override
    public Datum extract(TimeMeta tm) {
      return DatumFactory.createFloat8((double) tm.getWeekyear());
    }
  }

  private class MicrosecondsExtractorFromTimestamp implements DatePartExtractorFromTimestamp {
    @Override
    public Datum extract(TimeMeta tm) {
      return DatumFactory.createFloat8((double) (tm.secs * 1000000 + tm.fsecs));
    }
  }

  private class MillenniumExtractorFromTimestamp implements DatePartExtractorFromTimestamp {
    @Override
    public Datum extract(TimeMeta tm) {
      return DatumFactory.createFloat8((double) (((tm.years - 1) / 1000) + 1));
    }
  }

  private class MillisecondsExtractorFromTimestamp implements DatePartExtractorFromTimestamp {
    @Override
    public Datum extract(TimeMeta tm) {
      return DatumFactory.createFloat8((double) (tm.secs * 1000 + tm.fsecs / 1000.0));
    }
  }

  private class MinuteExtractorFromTimestamp implements DatePartExtractorFromTimestamp {
    @Override
    public Datum extract(TimeMeta tm) {
      return DatumFactory.createFloat8((double) tm.minutes);
    }
  }

  private class MonthExtractorFromTimestamp implements DatePartExtractorFromTimestamp {
    @Override
    public Datum extract(TimeMeta tm) {
      return DatumFactory.createFloat8((double) tm.monthOfYear);
    }
  }

  private class QuarterExtractorFromTimestamp implements DatePartExtractorFromTimestamp {
    @Override
    public Datum extract(TimeMeta tm) {
      return DatumFactory.createFloat8((double) (((tm.monthOfYear - 1) / 3) + 1));
    }
  }

  private class SecondExtractorFromTimestamp implements DatePartExtractorFromTimestamp {
    @Override
    public Datum extract(TimeMeta tm) {
      if (tm.fsecs != 0) {
        return DatumFactory.createFloat8(tm.secs + (((double) tm.fsecs) / (double) DateTimeConstants.USECS_PER_SEC));
      } else {
        return DatumFactory.createFloat8((double) tm.secs);
      }
    }
  }

  private class WeekExtractorFromTimestamp implements DatePartExtractorFromTimestamp {
    @Override
    public Datum extract(TimeMeta tm) {
      return DatumFactory.createFloat8((double) tm.getWeekOfYear());
    }
  }

  private class YearExtractorFromTimestamp implements DatePartExtractorFromTimestamp {
    @Override
    public Datum extract(TimeMeta tm) {
      return DatumFactory.createFloat8((double) tm.years);
    }
  }

  private class NullExtractorFromTimestamp implements DatePartExtractorFromTimestamp {
    @Override
    public Datum extract(TimeMeta tm) {
      return NullDatum.get();
    }
  }
}

