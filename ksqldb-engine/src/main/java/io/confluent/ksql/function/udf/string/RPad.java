/*
 * Copyright 2020 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"; you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and limitations under the
 * License.
 */

package io.confluent.ksql.function.udf.string;

import io.confluent.ksql.function.FunctionCategory;
import io.confluent.ksql.function.udf.Udf;
import io.confluent.ksql.function.udf.UdfDescription;
import io.confluent.ksql.function.udf.UdfParameter;
import io.confluent.ksql.util.BytesUtils;
import java.nio.ByteBuffer;
import java.util.Arrays;

@UdfDescription(
    name = "RPad",
    category = FunctionCategory.STRING,
    description = "Pads the input string or bytes, starting from the end, with the specified"
        + " padding string until the target length is reached. If the input string or bytes are"
        + " longer than the specified target length it will be truncated. If the padding string or"
        + " bytes are empty or NULL, or the target length is negative, then NULL is returned.")
public class RPad {

  @Udf
  public String rpad(
      @UdfParameter(description = "String to be padded") final String input,
      @UdfParameter(description = "Target length") final Integer targetLen,
      @UdfParameter(description = "Padding string") final String padding) {

    if (input == null) {
      return null;
    }
    if (padding == null || padding.isEmpty() || targetLen == null || targetLen < 0) {
      return null;
    }
    final StringBuilder sb = new StringBuilder(targetLen + padding.length());
    sb.append(input);
    final int padChars = Math.max(targetLen - input.length(), 0);
    for (int i = 0; i < padChars; i += padding.length()) {
      sb.append(padding);
    }
    sb.setLength(targetLen);
    return sb.toString();
  }

  @Udf
  public ByteBuffer rpad(
      @UdfParameter(description = "Bytes to be padded") final ByteBuffer input,
      @UdfParameter(description = "Target length") final Integer targetLen,
      @UdfParameter(description = "Padding bytes") final ByteBuffer padding) {

    if (input == null) {
      return null;
    }
    if (padding == null
        || BytesUtils.getByteArray(padding).length == 0
        || targetLen == null
        || targetLen < 0) {
      return null;
    }

    final byte[] start = BytesUtils.getByteArray(input);

    if (start.length > targetLen) {
      return ByteBuffer.wrap(Arrays.copyOfRange(start, 0, targetLen));
    }

    final byte[] padded = new byte[targetLen];
    final byte[] paddingArray = BytesUtils.getByteArray(padding);

    for (int i = 0; i < targetLen; i++) {
      if (i < start.length) {
        padded[i] = start[i];
      } else {
        final int paddingIndex = (i - start.length) % paddingArray.length;
        padded[i] = paddingArray[paddingIndex];
      }

    }
    return ByteBuffer.wrap(padded);
  }
}
