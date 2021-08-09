/*
 * Copyright 2019 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"); you may not use
 * this file except in compliance with the License.  You may obtain a copy of the
 * License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.ksql.schema.ksql;

import static io.confluent.ksql.schema.ksql.Column.Namespace.KEY;
import static io.confluent.ksql.schema.ksql.Column.Namespace.VALUE;
import static io.confluent.ksql.schema.ksql.SystemColumns.CURRENT_PSEUDOCOLUMN_VERSION_NUMBER;
import static io.confluent.ksql.schema.ksql.SystemColumns.ROWOFFSET_NAME;
import static io.confluent.ksql.schema.ksql.SystemColumns.ROWOFFSET_TYPE;
import static io.confluent.ksql.schema.ksql.SystemColumns.ROWPARTITION_NAME;
import static io.confluent.ksql.schema.ksql.SystemColumns.ROWPARTITION_ROWOFFSET_PSEUDOCOLUMN_VERSION;
import static io.confluent.ksql.schema.ksql.SystemColumns.ROWPARTITION_TYPE;
import static io.confluent.ksql.schema.ksql.SystemColumns.ROWTIME_NAME;
import static io.confluent.ksql.schema.ksql.SystemColumns.ROWTIME_PSEUDOCOLUMN_VERSION;
import static io.confluent.ksql.schema.ksql.SystemColumns.ROWTIME_TYPE;
import static io.confluent.ksql.schema.ksql.SystemColumns.WINDOWBOUND_TYPE;
import static io.confluent.ksql.schema.ksql.SystemColumns.WINDOWEND_NAME;
import static io.confluent.ksql.schema.ksql.SystemColumns.WINDOWSTART_NAME;

import com.google.common.collect.ImmutableList;
import com.google.errorprone.annotations.Immutable;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.schema.ksql.Column.Namespace;
import io.confluent.ksql.schema.ksql.types.SqlType;
import io.confluent.ksql.schema.utils.FormatOptions;
import io.confluent.ksql.util.DuplicateColumnException;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;

/**
 * Immutable KSQL logical schema.
 */
@Immutable
public final class LogicalSchema {

  private final ImmutableList<Column> columns;

  public static Builder builder() {
    return new Builder(ImmutableList.of());
  }

  private LogicalSchema(final ImmutableList<Column> columns) {
    this.columns = Objects.requireNonNull(columns, "columns");
  }

  public Builder asBuilder() {
    return new Builder(columns);
  }

  /**
   * @return the schema of the key.
   */
  public List<Column> key() {
    return byNamespace()
        .get(Namespace.KEY);
  }

  /**
   * @return the schema of the value.
   */
  public List<Column> value() {
    return byNamespace()
        .get(VALUE);
  }

  /**
   * @return all columns in the schema.
   */
  @SuppressFBWarnings(value = "EI_EXPOSE_REP", justification = "columns is ImmutableList")
  public List<Column> columns() {
    return columns;
  }

  /**
   * Search for a column with the supplied {@code columnRef}.
   *
   * @param columnName the column source and name to match.
   * @return the column if found, else {@code Optional.empty()}.
   */
  public Optional<Column> findColumn(final ColumnName columnName) {
    return findColumnMatching(withName(columnName));
  }

  /**
   * Search for a value column with the supplied {@code columnRef}.
   *
   * @param columnName the column source and name to match.
   * @return the value column if found, else {@code Optional.empty()}.
   */
  public Optional<Column> findValueColumn(final ColumnName columnName) {
    return findColumnMatching(withNamespace(VALUE).and(withName(columnName)));
  }

  /**
   * Checks to see if value namespace contain any of the supplied names.
   *
   * @param names the names to check for.
   * @return {@code true} if <i>any</i> of the supplied names exist in the value namespace.
   */
  public boolean valueContainsAny(final Set<ColumnName> names) {
    return value().stream()
        .map(Column::name)
        .anyMatch(names::contains);
  }

  /**
   * Copies pseudo and key columns to the value schema.
   *
   * <p>If the columns already exist in the value schema the function returns the same schema.
   *
   * @param windowed indicates that the source is windowed; meaning {@code WINDOWSTART} and {@code
   * WINDOWEND} columns will added to the value schema to represent the window bounds.
   *
   * @param pseudoColumnVersion determines the set of pseudocolumns to be added to the schema
   *
   * @return the new schema.
   */
  public LogicalSchema withPseudoAndKeyColsInValue(
      final boolean windowed, final int pseudoColumnVersion) {
    return rebuildWithPseudoAndKeyColsInValue(windowed, pseudoColumnVersion);
  }

  /**
   * Copies pseudo and key columns to the value schema with the current pseudocolumn version number
   *
   * <p>If the columns already exist in the value schema the function returns the same schema.
   *
   * @param windowed indicates that the source is windowed; meaning {@code WINDOWSTART} and {@code
   * WINDOWEND} columns will added to the value schema to represent the window bounds.
   **
   * @return the new schema.
   */
  public LogicalSchema withPseudoAndKeyColsInValue(final boolean windowed) {
    return withPseudoAndKeyColsInValue(windowed, CURRENT_PSEUDOCOLUMN_VERSION_NUMBER);
  }

  /**
   * Remove pseudo and key columns from the value schema, according to the pseudocolumn version from
   * the value schema.
   * @param pseudoColumnVersion the version of pseudocolumns to evaluate against
   * @return the new schema with the columns removed
   */
  LogicalSchema withoutPseudoAndKeyColsInValue(final int pseudoColumnVersion) {
    return rebuildWithoutPseudoAndKeyColsInValue(pseudoColumnVersion);
  }

  /**
   * Remove pseudo and key columns from the value schema.
   *
   * @return the new schema with the columns removed.
   */
  public LogicalSchema withoutPseudoAndKeyColsInValue() {
    return withoutPseudoAndKeyColsInValue(CURRENT_PSEUDOCOLUMN_VERSION_NUMBER);
  }

  /**
   * Remove all non-key columns from the value, and copy all key columns into the value.
   *
   * @return the new schema
   */
  public LogicalSchema withKeyColsOnly() {
    final List<Column> key = byNamespace().get(Namespace.KEY);

    final ImmutableList.Builder<Column> builder = ImmutableList.builder();
    builder.addAll(key);
    int valueIndex = 0;
    for (final Column c : key) {
      builder.add(Column.of(c.name(), c.type(), VALUE, valueIndex++));
    }

    return new LogicalSchema(builder.build());
  }

  /**
   * @param columnName the column name to check
   * @return {@code true} if the column matches the name of any key column.
   */
  public boolean isKeyColumn(final ColumnName columnName) {
    return findColumnMatching(withNamespace(Namespace.KEY).and(withName(columnName)))
        .isPresent();
  }

  /**
   * Returns True if this schema is compatible with {@code other} schema.
   */
  public boolean compatibleSchema(final LogicalSchema other) {
    if (columns().size() != other.columns().size()) {
      return false;
    }

    for (int i = 0; i < columns().size(); i++) {
      final Column s1Column = columns().get(i);
      final Column s2Column = other.columns().get(i);
      final SqlType s2Type = s2Column.type();

      if (!s1Column.equalsIgnoreType(s2Column) || !s1Column.canImplicitlyCast(s2Type)) {
        return false;
      }
    }

    return true;
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final LogicalSchema that = (LogicalSchema) o;
    return Objects.equals(columns, that.columns);
  }

  @Override
  public int hashCode() {
    return Objects.hash(columns);
  }

  @Override
  public String toString() {
    return toString(FormatOptions.none());
  }

  public String toString(final FormatOptions formatOptions) {
    return columns.stream()
        .map(c -> c.toString(formatOptions))
        .collect(Collectors.joining(", "));
  }

  private Optional<Column> findColumnMatching(final Predicate<Column> predicate) {
    // At the moment, it's possible for some column names to have multiple matches, e.g.
    // ROWKEY and ROWTIME. Order of preference on namespace is KEY then VALUE, as per Namespace
    // enum ordinal.

    return columns.stream()
        .filter(predicate)
        .min(Comparator.comparingInt(c -> c.namespace().ordinal()));
  }

  private Map<Namespace, List<Column>> byNamespace() {
    final Map<Namespace, List<Column>> byNamespace = columns.stream()
        .collect(Collectors.groupingBy(Column::namespace));

    Arrays.stream(Namespace.values())
        .forEach(ns -> byNamespace.putIfAbsent(ns, ImmutableList.of()));

    return byNamespace;
  }

  /**
   * Rebuilds schema with pseudocolumns and key columns included
   * @param windowedKey indicates if the schema to be rebuilt includes a windowed key
   * @param pseudoColumnVersion indicates which set of pseudocolumns should be used
   * @return the LogicalSchema created, with the corresponding pseudo and key columns included
   */
  private LogicalSchema rebuildWithPseudoAndKeyColsInValue(
      final boolean windowedKey, final int pseudoColumnVersion) {
    final Map<Namespace, List<Column>> byNamespace = byNamespace();

    final List<Column> key = byNamespace.get(Namespace.KEY);

    final ImmutableList.Builder<Column> builder = ImmutableList.builder();

    addKeyColumnsToKeySchema(builder);

    int valueIndex = addNonPseudoAndKeyColsToValueSchema(builder, pseudoColumnVersion);

    if (pseudoColumnVersion >= ROWTIME_PSEUDOCOLUMN_VERSION) {
      builder.add(Column.of(ROWTIME_NAME, ROWTIME_TYPE, VALUE, valueIndex++));
    }

    if (pseudoColumnVersion >= ROWPARTITION_ROWOFFSET_PSEUDOCOLUMN_VERSION) {
      builder.add(Column.of(ROWPARTITION_NAME, ROWPARTITION_TYPE, VALUE, valueIndex++));
      builder.add(Column.of(ROWOFFSET_NAME, ROWOFFSET_TYPE, VALUE, valueIndex++));
    }

    for (final Column c : key) {
      builder.add(Column.of(c.name(), c.type(), VALUE, valueIndex++));
    }

    if (windowedKey) {
      builder.add(
          Column.of(WINDOWSTART_NAME, WINDOWBOUND_TYPE, VALUE, valueIndex++));
      builder.add(
          Column.of(WINDOWEND_NAME, WINDOWBOUND_TYPE, VALUE, valueIndex));
    }


    return new LogicalSchema(builder.build());
  }

  /**
   * Rebuilds schema without pseudocolumns or key columns
   * @return the LogicalSchema created, with the corresponding pseudo and key columns excluded
   */
  private LogicalSchema rebuildWithoutPseudoAndKeyColsInValue(final int pseudoColumnVersion) {
    final ImmutableList.Builder<Column> builder = ImmutableList.builder();
    addKeyColumnsToKeySchema(builder);
    addNonPseudoAndKeyColsToValueSchema(builder, pseudoColumnVersion);
    return new LogicalSchema(builder.build());
  }

  /**
   * Adds columns, except for key and pseudocolumns, to a provided builder's value schema
   * and returns the number of added columns
   * @param builder the builder to be passed in. Columns that don't qualify as key or pseudocolumns
   *                will be added
   * @return the number of columns added. This also functions as a "column index", to pass into
   * builder.add()
   */
  private int addNonPseudoAndKeyColsToValueSchema(
      final ImmutableList.Builder<Column> builder, final int pseudoColumnVersion) {
    final Map<Namespace, List<Column>> byNamespace = byNamespace();

    final List<Column> value = byNamespace.get(VALUE);

    int addedColumns = 0;
    for (final Column c : value) {
      if (SystemColumns.isSystemColumn(c.name(), pseudoColumnVersion)) {
        continue;
      }

      if (findColumnMatching(withNamespace(Namespace.KEY).and(withName(c.name()))).isPresent()) {
        continue;
      }

      builder.add(Column.of(c.name(), c.type(), VALUE, addedColumns++));
    }
    return addedColumns;
  }

  /**
   * Adds key columns to key schema of a provided builder
   * @param builder the builder to be passed in. This builder will have key columns added to it's
   *                key schema.
   */
  private void addKeyColumnsToKeySchema(ImmutableList.Builder<Column> builder) {
    final Map<Namespace, List<Column>> byNamespace = byNamespace();
    final List<Column> key = byNamespace.get(Namespace.KEY);
    builder.addAll(key);
  }

  private static Predicate<Column> withName(final ColumnName name) {
    return c -> c.name().equals(name);
  }

  private static Predicate<Column> withNamespace(final Namespace ns) {
    return c -> c.namespace() == ns;
  }

  public static final class Builder {

    private final ImmutableList.Builder<Column> columns = ImmutableList.builder();
    private final Set<ColumnName> seenKeys = new HashSet<>();
    private final Set<ColumnName> seenValues = new HashSet<>();

    private Builder(final ImmutableList<Column> columns) {
      columns.forEach(col -> {
        if (col.namespace() == KEY) {
          keyColumn(col.name(), col.type());
        } else {
          valueColumn(col.name(), col.type());
        }
      });
    }

    public Builder keyColumns(final Iterable<? extends SimpleColumn> columns) {
      columns.forEach(this::keyColumn);
      return this;
    }

    public Builder keyColumn(final ColumnName columnName, final SqlType type) {
      addColumn(Column.of(columnName, type, Column.Namespace.KEY, seenKeys.size()));
      return this;
    }

    public Builder keyColumn(final SimpleColumn col) {
      return keyColumn(col.name(), col.type());
    }

    public Builder valueColumns(final Iterable<? extends SimpleColumn> column) {
      column.forEach(this::valueColumn);
      return this;
    }

    public Builder valueColumn(final SimpleColumn col) {
      return valueColumn(col.name(), col.type());
    }

    public Builder valueColumn(final ColumnName name, final SqlType type) {
      addColumn(Column.of(name, type, VALUE, seenValues.size()));
      return this;
    }

    public LogicalSchema build() {
      return new LogicalSchema(columns.build());
    }

    private void addColumn(final Column column) {
      switch (column.namespace()) {
        case KEY:
          if (!seenKeys.add(column.name())) {
            throw new DuplicateColumnException(column.namespace(), column);
          }
          break;

        case VALUE:
          if (!seenValues.add(column.name())) {
            throw new DuplicateColumnException(column.namespace(), column);
          }
          break;

        default:
          throw new UnsupportedOperationException("Unsupported column type: " + column);
      }

      columns.add(column);
    }
  }
}