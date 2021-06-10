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
import io.confluent.ksql.function.udf.Udf;
import io.confluent.ksql.function.udf.UdfDescription;
import io.confluent.ksql.function.udf.UdfParameter;
import io.confluent.ksql.function.udf.UdfSchemaProvider;
import io.confluent.ksql.schema.ksql.SqlArgument;
import io.confluent.ksql.schema.ksql.types.SqlBaseType;
import io.confluent.ksql.schema.ksql.types.SqlType;
import io.confluent.ksql.util.KsqlConstants;
import io.confluent.ksql.util.KsqlException;
import java.math.BigDecimal;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

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
  public Integer least(@UdfParameter final Integer... val) {
    //todo: throw a relevant exception or return null here
    return Arrays.stream(val)
        .filter(Objects::nonNull)
        .min(Integer::compareTo)
        .get();
  }

  @Udf
  public Long least(@UdfParameter final Long... val) {
    return Arrays.stream(val)
        .filter(Objects::nonNull)
        .min(Long::compareTo)
        .get();
  }

  @Udf
  public Double least(@UdfParameter final Double... val) {
    return Arrays.stream(val)
        .filter(Objects::nonNull)
        .min(Double::compareTo)
        .get();
  }

  //select least() from
  @Udf
  public String least(@UdfParameter final String... val) {
    return Arrays.stream(val)
        .filter(Objects::nonNull)
        .min(String::compareTo)
        .get();
  }


  @Udf(schemaProvider = "leastDecimalProvider")
  public BigDecimal least(@UdfParameter final BigDecimal... val) {
    //tentative code for checking precision and scale
    BigDecimal bd = val[0];

    if (Arrays.stream(val).anyMatch(b -> b.precision() != bd.precision() || b.scale() != bd.scale())){
      throw new KsqlException("The schema provider for Least(Decimal) expects provided arguments to match in precision and scale.");
    }


    return val == null || val.length == 0 ? //todo: move check up
        null :
        Arrays.stream(val)
            .filter(Objects::nonNull)
            .min(Comparator.naturalOrder())
            .get();

  }

  @UdfSchemaProvider
  public SqlType leastDecimalProvider(final List<SqlArgument> params) {
    if (
        params.stream()
            .map(SqlArgument::getSqlTypeOrThrow)
            .allMatch(s -> s.baseType() == SqlBaseType.DECIMAL)) {
      return params.get(0).getSqlTypeOrThrow();
    } else {
      throw new KsqlException("The schema provider for Least expects a BigDecimal parameter type.");
    }
  }
}