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
import java.math.RoundingMode;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;

@UdfDescription(
    name = "Greatest",
    category = FunctionCategory.MATHEMATICAL,
    description = "Returns the lowest value among a variable number of consistently typed columns.",
    author = KsqlConstants.CONFLUENT_AUTHOR
)
public class Greatest {

  //todo: how to verify that there is at least 1 argument?
  //todo: If I do verify, suppress warnings on get or no?

  @Udf
  public Integer greatest(@UdfParameter final Integer... val) {
    return Arrays.stream(val)
        .filter(Objects::nonNull)
        .max(Integer::compareTo)
        .get();
  }

  @Udf
  public Long greatest(@UdfParameter final Long... val) {
    return Arrays.stream(val)
        .filter(Objects::nonNull)
        .max(Long::compareTo)
        .get();
  }

  @Udf
  public Double greatest(@UdfParameter final Double... val) {
    return Arrays.stream(val)
        .filter(Objects::nonNull)
        .max(Double::compareTo)
        .get();
  }

  @Udf
  public String greatest(@UdfParameter final String... val){
    return Arrays.stream(val)
        .filter(Objects::nonNull)
        .max(String::compareTo)
        .get();
  }


  //fixme: use bigDecimal.setScale()
  @Udf(schemaProvider = "leastDecimalProvider")
  public BigDecimal greatest(@UdfParameter final BigDecimal... val) {
    //tentative code for checking precision and scale

    BigDecimal bd = val[0];

    if (Arrays.stream(val).anyMatch(b -> b.precision() != bd.precision() || b.scale() != bd.scale())){
      throw new KsqlException("The schema provider for Greatest(Decimal) expects provided arguments to match in precision and scale.");
    }


    return val == null || val.length == 0 ? //todo: move check up
        null :
        Arrays.stream(val)
            .filter(Objects::nonNull)
            .max(Comparator.naturalOrder())
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
      throw new KsqlException("The schema provider for Greatest expects a BigDecimal parameter type.");
    }
  }

}
