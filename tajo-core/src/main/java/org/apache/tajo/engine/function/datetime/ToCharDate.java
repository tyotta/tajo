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
import org.apache.tajo.engine.function.annotation.Description;
import org.apache.tajo.engine.function.annotation.ParamTypes;
import org.apache.tajo.plan.expr.FunctionEval;
import org.apache.tajo.plan.function.GeneralFunction;
import org.apache.tajo.storage.Tuple;
import org.apache.tajo.util.datetime.DateTimeFormat;
import org.apache.tajo.util.datetime.DateTimeUtil;
import org.apache.tajo.util.datetime.TimeMeta;

import static org.apache.tajo.common.TajoDataTypes.Type.DATE;
import static org.apache.tajo.common.TajoDataTypes.Type.TEXT;

@Description(
    functionName = "to_char",
    description = "Convert date to string. Format should be a SQL standard format string.",
    example = "> SELECT to_char(DATE '2014-01-17', 'YYYY-MM');\n"
        + "2014-01",
    returnType = TajoDataTypes.Type.TEXT,
    paramTypes = {@ParamTypes(paramTypes = {TajoDataTypes.Type.DATE, TajoDataTypes.Type.TEXT})}
)
public class ToCharDate extends GeneralFunction {
  public ToCharDate() {
    super(new Column[] {
        new Column("date", DATE),
        new Column("format", TEXT)
    });
  }

  @Override
  public void init(FunctionEval.ParamType[] paramTypes) {
  }

  @Override
  public Datum eval(Tuple params) {
    if(params.isNull(0) || params.isNull(1)) {
      return NullDatum.get();
    }

    DateDatum valueDatum = (DateDatum) params.get(0);
    TimeMeta tm = valueDatum.toTimeMeta();
    DateTimeUtil.toUserTimezone(tm);

    Datum pattern = params.get(1);

    return DatumFactory.createText(DateTimeFormat.to_char(tm, pattern.asChars()));
  }
}