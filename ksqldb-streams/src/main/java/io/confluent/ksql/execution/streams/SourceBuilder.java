/*
 * Copyright 2019 Confluent Inc.
 *
 * Licensed under the Confluent Community License; you may not use this file
 * except in compliance with the License.  You may obtain a copy of the License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.ksql.execution.streams;

import static io.confluent.ksql.execution.streams.SourceBuilderUtils.AddKeyAndPseudoColumns;
import static io.confluent.ksql.execution.streams.SourceBuilderUtils.changelogTopic;
import static io.confluent.ksql.execution.streams.SourceBuilderUtils.getRegisterCallback;
import static java.util.Objects.requireNonNull;

import io.confluent.ksql.GenericKey;
import io.confluent.ksql.GenericRow;
import io.confluent.ksql.execution.context.QueryContext;
import io.confluent.ksql.execution.plan.Formats;
import io.confluent.ksql.execution.plan.KTableHolder;
import io.confluent.ksql.execution.plan.PlanInfo;
import io.confluent.ksql.execution.plan.SourceStep;
import io.confluent.ksql.execution.plan.WindowedTableSource;
import io.confluent.ksql.execution.runtime.RuntimeBuildContext;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.schema.ksql.PhysicalSchema;
import io.confluent.ksql.schema.ksql.SystemColumns;
import io.confluent.ksql.serde.FormatInfo;
import io.confluent.ksql.serde.KeyFormat;
import io.confluent.ksql.serde.SerdeFeatures;
import io.confluent.ksql.serde.StaticTopicSerde;
import io.confluent.ksql.serde.StaticTopicSerde.Callback;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.function.Function;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.ValueTransformerWithKey;
import org.apache.kafka.streams.kstream.ValueTransformerWithKeySupplier;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;

final class SourceBuilder extends SourceBuilderBase{

  private final static SourceBuilder instance;

  static {
    instance = new SourceBuilder();
  }

  private SourceBuilder() {
  }

  public static SourceBuilder instance() {
    return instance;
  }

  @Override
  <K> KTable<K, GenericRow> buildKTable(
      final SourceStep<?> streamSource,
      final RuntimeBuildContext buildContext,
      final Consumed<K, GenericRow> consumed,
      final Function<K, Collection<?>> keyGenerator,
      final Materialized<K, GenericRow, KeyValueStore<Bytes, byte[]>> materialized,
      final Serde<GenericRow> valueSerde,
      final String stateStoreName,
      final PlanInfo planInfo
  ) {

    final KTable<K, GenericRow> table;

      final KTable<K, GenericRow> source = buildContext
          .getStreamsBuilder()
          .table(streamSource.getTopicName(), consumed);

      final boolean forceMaterialization = !planInfo.isRepartitionedInPlan(streamSource);

      final KTable<K, GenericRow> transformed = source.transformValues(
          new AddPseudoColumnsToMaterialize<>(streamSource.getPseudoColumnVersion()));

      if (forceMaterialization) {
        // add this identity mapValues call to prevent the source-changelog
        // optimization in kafka streams - we don't want this optimization to
        // be enabled because we cannot require symmetric serialization between
        // producer and KSQL (see https://issues.apache.org/jira/browse/KAFKA-10179
        // and https://github.com/confluentinc/ksql/issues/5673 for more details)
        table = transformed.mapValues(row -> row, materialized);
      } else {
        // if we know this table source is repartitioned later in the topology,
        // we do not need to force a materialization at this source step since the
        // re-partitioned topic will be used for any subsequent state stores, in lieu
        // of the original source topic, thus avoiding the issues above.
        // See https://github.com/confluentinc/ksql/issues/6650
        table = transformed.mapValues(row -> row);
      }

    return table.transformValues(new AddRemainingPseudoAndKeyCols<>(
        keyGenerator,
        streamSource.getPseudoColumnVersion()));
  }

  @Override
  <K> KTable<K, GenericRow> buildWindowedKTable(SourceStep<?> streamSource,
      final RuntimeBuildContext buildContext,
      final Consumed<K, GenericRow> consumed,
      final Function<K, Collection<?>> keyGenerator,
      final Materialized<K, GenericRow, KeyValueStore<Bytes, byte[]>> materialized,
      final Serde<GenericRow> valueSerde,
      final String stateStoreName,
      final PlanInfo planInfo
  ) {
    final String changelogTopic = changelogTopic(buildContext, stateStoreName);
    final Callback onFailure = getRegisterCallback(
        buildContext, streamSource.getFormats().getValueFormat());

    final KTable<K, GenericRow> table = buildContext
        .getStreamsBuilder()
        .table(
            streamSource.getTopicName(),
            consumed.withValueSerde(StaticTopicSerde.wrap(changelogTopic, valueSerde, onFailure)),
            materialized
        );

    return table
        .transformValues(new AddKeyAndPseudoColumns<>(
            keyGenerator, streamSource.getPseudoColumnVersion()));
  }

  @Override
  Materialized<GenericKey, GenericRow, KeyValueStore<Bytes, byte[]>> buildTableMaterialized(
      final SourceStep<KTableHolder<GenericKey>> source,
      final RuntimeBuildContext buildContext,
      final MaterializedFactory materializedFactory,
      final Serde<GenericKey> doNotUse,
      final Serde<GenericRow> doNotUse2
  ) {

    final String stateStoreName = SourceBuilderUtils.tableChangeLogOpName(source.getProperties());

    final PhysicalSchema physicalSchema = getPhysicalSchemaWithPseudoColumnsToMaterialize(source);

    final QueryContext queryContext = QueryContext.Stacker.of(
        source.getProperties().getQueryContext())
        .push("Materialize").getQueryContext();

    final Serde<GenericRow> valueSerde = getValueSerdeWithAdditionalQueryContext(
        buildContext, source, physicalSchema, queryContext);

    final Serde<GenericKey> keySerde = buildContext.buildKeySerde(
        source.getFormats().getKeyFormat(),
        physicalSchema,
        queryContext
    );

    return materializedFactory.create(
        keySerde,
        valueSerde,
        stateStoreName
    );
  }

  @Override
  Materialized<Windowed<GenericKey>, GenericRow, KeyValueStore<Bytes, byte[]>>
  buildWindowedTableMaterialized(
      final SourceStep<KTableHolder<Windowed<GenericKey>>> source,
      final RuntimeBuildContext buildContext,
      final MaterializedFactory materializedFactory,
      final Serde<Windowed<GenericKey>> doNotUse,
      final Serde<GenericRow> doNotUse2
  ) {

    final String stateStoreName = SourceBuilderUtils.tableChangeLogOpName(source.getProperties());

    final PhysicalSchema physicalSchema = getPhysicalSchemaWithKeyAndPseudoCols(source);

    final QueryContext queryContext = QueryContext.Stacker.of(
        source.getProperties().getQueryContext())
        .push("Materialize").getQueryContext();

    final Serde<GenericRow> valueSerde = getValueSerdeWithAdditionalQueryContext(
        buildContext, source, physicalSchema, queryContext);

    final Serde<Windowed<GenericKey>> keySerde = buildContext.buildKeySerde(
        source.getFormats().getKeyFormat(),
        ((WindowedTableSource) source).getWindowInfo(),
        physicalSchema,
        queryContext
    );

    return materializedFactory.create(
        keySerde,
        valueSerde,
        stateStoreName
    );
  }

  private static Serde<GenericRow> getValueSerdeWithAdditionalQueryContext(
      final RuntimeBuildContext buildContext,
      final SourceStep<?> streamSource,
      final PhysicalSchema physicalSchema,
      final QueryContext queryContext) {

    return buildContext.buildValueSerde(
        streamSource.getFormats().getValueFormat(),
        physicalSchema,
        queryContext
    );
  }

  private static PhysicalSchema getPhysicalSchemaWithKeyAndPseudoCols(
      final SourceStep<?> streamSource) {

    final boolean windowed = streamSource instanceof WindowedTableSource;

    final FormatInfo formatInfo = streamSource.getFormats().getKeyFormat();
    final SerdeFeatures serdeFeatures = streamSource.getFormats().getKeyFeatures();
    final KeyFormat keyFormat = windowed
        ? KeyFormat.windowed(
            formatInfo, serdeFeatures, ((WindowedTableSource) streamSource).getWindowInfo())
        : KeyFormat.nonWindowed(formatInfo, serdeFeatures);

    final Formats formats = of(keyFormat, streamSource.getFormats().getValueFormat());

    return PhysicalSchema.from(
        streamSource.getSourceSchema().withPseudoAndKeyColsInValue(
            windowed, streamSource.getPseudoColumnVersion()),
        formats.getKeyFeatures(),
        formats.getValueFeatures()
    );
  }

  private static PhysicalSchema getPhysicalSchemaWithPseudoColumnsToMaterialize(
      final SourceStep<?> streamSource) {

    FormatInfo f = streamSource.getFormats().getKeyFormat();
    SerdeFeatures s = streamSource.getFormats().getKeyFeatures();
    KeyFormat k = streamSource instanceof WindowedTableSource
        ? KeyFormat.windowed(f, s, ((WindowedTableSource) streamSource).getWindowInfo())
        : KeyFormat.nonWindowed(f, s);

    Formats format = of(k, streamSource.getFormats().getValueFormat());

    LogicalSchema withPseudosToMaterialize
        = streamSource.getSourceSchema().withPseudoColumnsToMaterialize(
        false, streamSource.getPseudoColumnVersion());

    return PhysicalSchema.from(
        withPseudosToMaterialize,
        format.getKeyFeatures(),
        format.getValueFeatures()
    );
  }

  //todo: put this logic into TableSource and WindowedTableSource
  private static Formats of(final KeyFormat keyFormat, final FormatInfo valueFormat) {

    return Formats.of(
        keyFormat.getFormatInfo(),
        valueFormat,
        keyFormat.getFeatures(),
        SerdeFeatures.of()
    );
  }

  private static class AddRemainingPseudoAndKeyCols<K>
      implements ValueTransformerWithKeySupplier<K, GenericRow, GenericRow> {

    private final Function<K, Collection<?>> keyGenerator;
    private final int pseudoColumnVersion;

    AddRemainingPseudoAndKeyCols(
        final Function<K, Collection<?>> keyGenerator, final int pseudoColumnVersion) {
      this.keyGenerator = requireNonNull(keyGenerator, "keyGenerator");
      this.pseudoColumnVersion = pseudoColumnVersion;
    }

    @Override
    public ValueTransformerWithKey<K, GenericRow, GenericRow> get() {
      return new ValueTransformerWithKey<K, GenericRow, GenericRow>() {
        private ProcessorContext processorContext;

        @Override
        public void init(final ProcessorContext processorContext) {
          this.processorContext = requireNonNull(processorContext, "processorContext");
        }

        @Override
        public GenericRow transform(final K key, final GenericRow row) {
          if (row == null) {
            return row;
          }

          final Collection<?> keyColumns = keyGenerator.apply(key);

          //remove pseudocolumns we previously materialized so we can add them back in correct order

          //ensure extra capacity equal to number of pseudoColumns which we haven't materialized
          final int pseudoColumnsToAdd = SystemColumns.pseudoColumnNames(
              SystemColumns.ROWTIME_PSEUDOCOLUMN_VERSION).size();

          row.ensureAdditionalCapacity(pseudoColumnsToAdd);

          //calculate number of user columns and
          final int totalPseudoColumns = SystemColumns.pseudoColumnNames(pseudoColumnVersion).size();
          final int pseudoColumnsToShift = totalPseudoColumns - pseudoColumnsToAdd;
          final int numUserColumns = row.size() - pseudoColumnsToShift;

          Object toShift = processorContext.timestamp();

          for (int i = numUserColumns; i < row.size(); i++) {
            Object temp = row.get(i);
            row.set(i, toShift);
            toShift = temp;
          }

          row.append(toShift);

          row.appendAll(keyColumns);
          return row;
        }

        @Override
        public void close() {
        }
      };
    }
  }


  private static class AddPseudoColumnsToMaterialize<K>
      implements ValueTransformerWithKeySupplier<K, GenericRow, GenericRow> {

    private final int pseudoColumnVersion;

    AddPseudoColumnsToMaterialize(final int pseudoColumnVersion) {
      this.pseudoColumnVersion = pseudoColumnVersion;
    }

    @Override
    public ValueTransformerWithKey<K, GenericRow, GenericRow> get() {
      return new ValueTransformerWithKey<K, GenericRow, GenericRow>() {
        private ProcessorContext processorContext;

        @Override
        public void init(final ProcessorContext processorContext) {
          this.processorContext = requireNonNull(processorContext, "processorContext");
        }

        @Override
        public GenericRow transform(final K key, final GenericRow row) {
          if (row == null) {
            return row;
          }

          final int numPseudoColumns = SystemColumns
              .pseudoColumnNames(pseudoColumnVersion).size();

          row.ensureAdditionalCapacity(numPseudoColumns - 1);

          if (pseudoColumnVersion >= SystemColumns.ROWPARTITION_ROWOFFSET_PSEUDOCOLUMN_VERSION) {
            final int partition = processorContext.partition();
            final long offset = processorContext.offset();
            row.append(partition);
            row.append(offset);
          }

          return row;
        }

        @Override
        public void close() {
        }
      };
    }
  }

}
