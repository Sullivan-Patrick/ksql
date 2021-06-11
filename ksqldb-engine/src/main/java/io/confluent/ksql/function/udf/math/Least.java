/*
 * Copyright 2021 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"; you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.ksql.function.udf.math;

import io.confluent.ksql.function.FunctionCategory;
import io.confluent.ksql.function.KsqlFunctionException;
import io.confluent.ksql.function.udf.Udf;
import io.confluent.ksql.function.udf.UdfDescription;
import io.confluent.ksql.function.udf.UdfParameter;
import io.confluent.ksql.function.udf.UdfSchemaProvider;
import io.confluent.ksql.schema.ksql.SqlArgument;
import io.confluent.ksql.schema.ksql.types.SqlBaseType;
import io.confluent.ksql.schema.ksql.types.SqlDecimal;
import io.confluent.ksql.schema.ksql.types.SqlType;
import io.confluent.ksql.util.KsqlConstants;
import io.confluent.ksql.util.KsqlException;
import java.math.BigDecimal;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

@UdfDescription(
    name = "Least",
    category = FunctionCategory.MATHEMATICAL,
    description = "Returns the lowest value among a variable number of consistently typed columns.",
    author = KsqlConstants.CONFLUENT_AUTHOR
)
public class Least {

  //todo: how to verify that there is at least 1 argument?
  //todo: suppress warnings on get or no?

  @Udf
  public Integer least(@UdfParameter final Integer val, @UdfParameter final Integer... vals) {
    //todo: throw a relevant exception or return null here
    return Math.min(val, Arrays.stream(vals)
        .min(Integer::compareTo)
        .get());
  }

  @Udf
  public Long least(@UdfParameter final Long val, @UdfParameter final Long... vals) {
    return Math.min(val, Arrays.stream(vals)
        .min(Long::compareTo)
        .get());
  }

  @Udf
  public Double least(@UdfParameter final Double val, @UdfParameter final Double... vals) {
    return Math.min(val, Arrays.stream(vals)
        .min(Double::compareTo)
        .get());
  }

  @Udf
  public String least(@UdfParameter final String val, @UdfParameter final String... vals) {
    String lowestInArr = (Arrays.stream(vals)
        .min(String::compareTo)
        .get());

    return val.compareTo(lowestInArr) < 0 ? val : lowestInArr;

  }

  @Udf(schemaProvider = "leastDecimalProvider")
  public BigDecimal least(@UdfParameter final BigDecimal val, @UdfParameter final BigDecimal... vals) {
        return val.min(Arrays.stream(vals)
            .min(Comparator.naturalOrder())
            .get());
  }

  @UdfSchemaProvider
  public SqlType leastDecimalProvider(final List<SqlArgument> params) {
    if (params.get(0).getSqlTypeOrThrow().baseType() != SqlBaseType.DECIMAL){
      throw new KsqlException("The schema provider for Least expects a BigDecimal parameter type.");
    }

    SqlDecimal firstDecimal = (SqlDecimal) params.get(0).getSqlTypeOrThrow();

    if (
        params.stream()
            .map(SqlArgument::getSqlTypeOrThrow)
            .allMatch(s-> s.baseType() == SqlBaseType.DECIMAL && s.equals(firstDecimal)))
    {
      return params.get(0).getSqlTypeOrThrow();
    } else {
      throw new KsqlFunctionException("The schema provider for Least expects a BigDecimal parameter type.");
    }
  }


}