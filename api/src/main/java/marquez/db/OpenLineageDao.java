/*
 * Copyright 2018-2023 contributors to the Marquez project
 * SPDX-License-Identifier: Apache-2.0
 */

package marquez.db;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import java.net.URI;
import java.net.URISyntaxException;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import marquez.common.Utils;
import marquez.common.models.DatasetId;
import marquez.common.models.DatasetName;
import marquez.common.models.DatasetType;
import marquez.common.models.JobType;
import marquez.common.models.NamespaceName;
import marquez.common.models.RunState;
import marquez.common.models.SourceType;
import marquez.db.DatasetFieldDao.DatasetFieldMapping;
import marquez.db.JobVersionDao.BagOfJobVersionInfo;
import marquez.db.RunDao.RunUpsert;
import marquez.db.RunDao.RunUpsert.RunUpsertBuilder;
import marquez.db.mappers.LineageEventMapper;
import marquez.db.models.ColumnLineageRow;
import marquez.db.models.DatasetFieldRow;
import marquez.db.models.DatasetRow;
import marquez.db.models.DatasetSymlinkRow;
import marquez.db.models.DatasetVersionRow;
import marquez.db.models.InputFieldData;
import marquez.db.models.JobRow;
import marquez.db.models.ModelDaos;
import marquez.db.models.NamespaceRow;
import marquez.db.models.RunArgsRow;
import marquez.db.models.RunRow;
import marquez.db.models.RunStateRow;
import marquez.db.models.SourceRow;
import marquez.db.models.UpdateLineageRow;
import marquez.db.models.UpdateLineageRow.DatasetRecord;
import marquez.service.models.BaseEvent;
import marquez.service.models.DatasetEvent;
import marquez.service.models.LineageEvent;
import marquez.service.models.LineageEvent.Dataset;
import marquez.service.models.LineageEvent.DatasetFacets;
import marquez.service.models.LineageEvent.DocumentationJobFacet;
import marquez.service.models.LineageEvent.Job;
import marquez.service.models.LineageEvent.JobFacet;
import marquez.service.models.LineageEvent.LifecycleStateChangeFacet;
import marquez.service.models.LineageEvent.NominalTimeRunFacet;
import marquez.service.models.LineageEvent.ParentRunFacet;
import marquez.service.models.LineageEvent.Run;
import marquez.service.models.LineageEvent.RunFacet;
import marquez.service.models.LineageEvent.SchemaDatasetFacet;
import marquez.service.models.LineageEvent.SchemaField;
import org.apache.commons.lang3.tuple.Pair;
import org.jdbi.v3.sqlobject.config.RegisterRowMapper;
import org.jdbi.v3.sqlobject.statement.SqlQuery;
import org.jdbi.v3.sqlobject.statement.SqlUpdate;
import org.postgresql.util.PGobject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@RegisterRowMapper(LineageEventMapper.class)
public interface OpenLineageDao extends BaseDao {
  String DEFAULT_SOURCE_NAME = "default";
  String DEFAULT_NAMESPACE_OWNER = "anonymous";

  @SqlUpdate(
      "INSERT INTO lineage_events ("
          + "event_type, "
          + "event_time, "
          + "run_uuid, "
          + "job_name, "
          + "job_namespace, "
          + "event, "
          + "producer) "
          + "VALUES (?, ?, ?, ?, ?, ?, ?)")
  void createLineageEvent(
      String eventType,
      Instant eventTime,
      UUID runUuid,
      String jobName,
      String jobNamespace,
      PGobject event,
      String producer);

  @SqlUpdate(
      "INSERT INTO lineage_events ("
          + "event_time, "
          + "event, "
          + "producer) "
          + "VALUES (?, ?, ?)")
  void createDatasetEvent(Instant eventTime, PGobject event, String producer);

  @SqlQuery("SELECT event FROM lineage_events WHERE run_uuid = :runUuid")
  List<LineageEvent> findLineageEventsByRunUuid(UUID runUuid);

  @SqlQuery(
      """
  SELECT event
  FROM lineage_events le
  WHERE (le.event_time < :before
  AND le.event_time >= :after)
  ORDER BY le.event_time DESC
  LIMIT :limit OFFSET :offset""")
  List<LineageEvent> getAllLineageEventsDesc(
      ZonedDateTime before, ZonedDateTime after, int limit, int offset);

  @SqlQuery(
      """
  SELECT event
  FROM lineage_events le
  WHERE (le.event_time < :before
  AND le.event_time >= :after)
  ORDER BY le.event_time ASC
  LIMIT :limit OFFSET :offset""")
  List<LineageEvent> getAllLineageEventsAsc(
      ZonedDateTime before, ZonedDateTime after, int limit, int offset);

  @SqlQuery(
      """
      SELECT count(*)
      FROM lineage_events le
      WHERE (le.event_time < :before
      AND le.event_time >= :after)""")
  int getAllLineageTotalCount(ZonedDateTime before, ZonedDateTime after);

  default UpdateLineageRow updateMarquezModel(LineageEvent event, ObjectMapper mapper) {
    UpdateLineageRow updateLineageRow = updateBaseMarquezModel(event, mapper);
    RunState runState = getRunState(event.getEventType());
    if (event.getEventType() != null && runState.isDone()) {
      updateMarquezOnComplete(event, updateLineageRow, runState);
    }
    return updateLineageRow;
  }

  default UpdateLineageRow updateMarquezModel(DatasetEvent event, ObjectMapper mapper) {
    ModelDaos daos = new ModelDaos();
    daos.initBaseDao(this);
    Instant now = event.getEventTime().withZoneSameInstant(ZoneId.of("UTC")).toInstant();

    UpdateLineageRow bag = new UpdateLineageRow();
    NamespaceRow namespace =
        daos.getNamespaceDao()
            .upsertNamespaceRow(
                UUID.randomUUID(),
                now,
                formatNamespaceName(event.getDataset().getNamespace()),
                DEFAULT_NAMESPACE_OWNER);
    bag.setNamespace(namespace);

    Dataset dataset = event.getDataset();
    List<DatasetRecord> datasetOutputs = new ArrayList<>();
    DatasetRecord record = upsertLineageDataset(daos, dataset, now, null, false);
    datasetOutputs.add(record);
    insertDatasetFacets(daos, dataset, record, null, null, now);
    insertOutputDatasetFacets(daos, dataset, record, null, null, now);

    daos.getDatasetDao()
        .updateVersion(
            record.getDatasetVersionRow().getDatasetUuid(),
            Instant.now(),
            record.getDatasetVersionRow().getUuid());

    bag.setOutputs(Optional.ofNullable(datasetOutputs));
    return bag;
  }

  default UpdateLineageRow updateBaseMarquezModel(LineageEvent event, ObjectMapper mapper) {
    ModelDaos daos = new ModelDaos();
    daos.initBaseDao(this);
    Instant now = event.getEventTime().withZoneSameInstant(ZoneId.of("UTC")).toInstant();

    UpdateLineageRow bag = new UpdateLineageRow();
    NamespaceRow namespace =
        daos.getNamespaceDao()
            .upsertNamespaceRow(
                UUID.randomUUID(),
                now,
                formatNamespaceName(event.getJob().getNamespace()),
                DEFAULT_NAMESPACE_OWNER);
    bag.setNamespace(namespace);

    Instant nominalStartTime = getNominalStartTime(event);
    Instant nominalEndTime = getNominalEndTime(event);

    Optional<ParentRunFacet> parentRun =
        Optional.ofNullable(event.getRun()).map(Run::getFacets).map(RunFacet::getParent);
    Optional<UUID> parentUuid = parentRun.map(Utils::findParentRunUuid);

    JobRow job =
        buildJobFromEvent(
            event,
            mapper,
            daos.getJobDao(),
            now,
            namespace,
            nominalStartTime,
            nominalEndTime,
            parentRun);

    bag.setJob(job);

    Map<String, String> runArgsMap = createRunArgs(event);
    RunArgsRow runArgs =
        daos.getRunArgsDao()
            .upsertRunArgs(
                UUID.randomUUID(), now, Utils.toJson(runArgsMap), Utils.checksumFor(runArgsMap));
    bag.setRunArgs(runArgs);

    final UUID runUuid = runToUuid(event.getRun().getRunId());
    RunRow run;
    RunUpsertBuilder runUpsertBuilder =
        RunUpsert.builder()
            .runUuid(runUuid)
            .parentRunUuid(parentUuid.orElse(null))
            .externalId(event.getRun().getRunId())
            .now(now)
            .jobUuid(job.getUuid())
            .jobVersionUuid(null)
            .runArgsUuid(runArgs.getUuid())
            .namespaceName(namespace.getName())
            .jobName(job.getName())
            .location(job.getLocation());

    if (event.getEventType() != null) {
      runUpsertBuilder.runStateType(getRunState(event.getEventType())).runStateTime(now);
    }
    run = daos.getRunDao().upsert(runUpsertBuilder.build());
    insertRunFacets(daos, event, runUuid, now);
    bag.setRun(run);

    if (event.getEventType() != null) {
      RunState runStateType = getRunState(event.getEventType());

      RunStateRow runState =
          daos.getRunStateDao().upsert(UUID.randomUUID(), now, run.getUuid(), runStateType);
      bag.setRunState(runState);
      if (runStateType.isDone()) {
        daos.getRunDao().updateEndState(run.getUuid(), now, runState.getUuid());
      } else if (runStateType.isStarting()) {
        daos.getRunDao().updateStartState(run.getUuid(), now, runState.getUuid());
      }
    }

    insertJobFacets(daos, event, job.getUuid(), runUuid, now);

    // RunInput list uses null as a sentinel value
    List<DatasetRecord> datasetInputs = null;
    if (event.getInputs() != null) {
      datasetInputs = new ArrayList<>();
      for (Dataset dataset : event.getInputs()) {
        DatasetRecord record = upsertLineageDataset(daos, dataset, now, runUuid, true);
        datasetInputs.add(record);
        insertDatasetFacets(daos, dataset, record, runUuid, event.getEventType(), now);
        insertInputDatasetFacets(daos, dataset, record, runUuid, event.getEventType(), now);
      }
    }
    bag.setInputs(Optional.ofNullable(datasetInputs));

    // RunInput list uses null as a sentinel value
    List<DatasetRecord> datasetOutputs = null;
    if (event.getOutputs() != null) {
      datasetOutputs = new ArrayList<>();
      for (Dataset dataset : event.getOutputs()) {
        DatasetRecord record = upsertLineageDataset(daos, dataset, now, runUuid, false);
        datasetOutputs.add(record);
        insertDatasetFacets(daos, dataset, record, runUuid, event.getEventType(), now);
        insertOutputDatasetFacets(daos, dataset, record, runUuid, event.getEventType(), now);
      }
    }

    bag.setOutputs(Optional.ofNullable(datasetOutputs));
    return bag;
  }

  private static Instant getNominalStartTime(LineageEvent event) {
    return Optional.ofNullable(event.getRun().getFacets())
        .flatMap(f -> Optional.ofNullable(f.getNominalTime()))
        .map(NominalTimeRunFacet::getNominalStartTime)
        .map(t -> t.withZoneSameInstant(ZoneId.of("UTC")).toInstant())
        .orElse(null);
  }

  private static Instant getNominalEndTime(LineageEvent event) {
    return Optional.ofNullable(event.getRun().getFacets())
        .flatMap(f -> Optional.ofNullable(f.getNominalTime()))
        .map(NominalTimeRunFacet::getNominalEndTime)
        .map(t -> t.withZoneSameInstant(ZoneId.of("UTC")).toInstant())
        .orElse(null);
  }

  private void insertRunFacets(ModelDaos daos, LineageEvent event, UUID runUuid, Instant now) {
    // Add ...
    Optional.ofNullable(event.getRun().getFacets())
        .ifPresent(
            runFacet ->
                daos.getRunFacetsDao()
                    .insertRunFacetsFor(
                        runUuid, now, event.getEventType(), event.getRun().getFacets()));
  }

  private void insertJobFacets(
      ModelDaos daos, LineageEvent event, UUID jobUuid, UUID runUuid, Instant now) {
    // Add ...
    Optional.ofNullable(event.getJob().getFacets())
        .ifPresent(
            jobFacet ->
                daos.getJobFacetsDao()
                    .insertJobFacetsFor(
                        jobUuid, runUuid, now, event.getEventType(), event.getJob().getFacets()));
  }

  private void insertDatasetFacets(
      ModelDaos daos,
      Dataset dataset,
      DatasetRecord record,
      UUID runUuid,
      String eventType,
      Instant now) {
    // Facets ...
    Optional.ofNullable(dataset.getFacets())
        .ifPresent(
            facets ->
                daos.getDatasetFacetsDao()
                    .insertDatasetFacetsFor(
                        record.getDatasetRow().getUuid(),
                        record.getDatasetVersionRow().getUuid(),
                        runUuid,
                        now,
                        eventType,
                        facets));
  }

  private void insertInputDatasetFacets(
      ModelDaos daos,
      Dataset dataset,
      DatasetRecord record,
      UUID runUuid,
      String eventType,
      Instant now) {
    // InputFacets ...
    Optional.ofNullable(dataset.getInputFacets())
        .ifPresent(
            facets ->
                daos.getDatasetFacetsDao()
                    .insertInputDatasetFacetsFor(
                        record.getDatasetRow().getUuid(),
                        record.getDatasetVersionRow().getUuid(),
                        runUuid,
                        now,
                        eventType,
                        facets));
  }

  private void insertOutputDatasetFacets(
      ModelDaos daos,
      Dataset dataset,
      DatasetRecord record,
      UUID runUuid,
      String eventType,
      Instant now) {
    // OutputFacets ...
    Optional.ofNullable(dataset.getOutputFacets())
        .ifPresent(
            facets ->
                daos.getDatasetFacetsDao()
                    .insertOutputDatasetFacetsFor(
                        record.getDatasetRow().getUuid(),
                        record.getDatasetVersionRow().getUuid(),
                        runUuid,
                        now,
                        eventType,
                        facets));
  }

  private JobRow buildJobFromEvent(
      LineageEvent event,
      ObjectMapper mapper,
      JobDao jobDao,
      Instant now,
      NamespaceRow namespace,
      Instant nominalStartTime,
      Instant nominalEndTime,
      Optional<ParentRunFacet> parentRun) {
    Logger log = LoggerFactory.getLogger(OpenLineageDao.class);
    String description =
        Optional.ofNullable(event.getJob().getFacets())
            .map(JobFacet::getDocumentation)
            .map(DocumentationJobFacet::getDescription)
            .orElse(null);

    String location =
        Optional.ofNullable(event.getJob().getFacets())
            .flatMap(f -> Optional.ofNullable(f.getSourceCodeLocation()))
            .flatMap(s -> Optional.ofNullable(s.getUrl()))
            .orElse(null);

    Optional<UUID> parentUuid = parentRun.map(Utils::findParentRunUuid);
    Optional<JobRow> parentJob =
        parentUuid.map(
            uuid ->
                findParentJobRow(
                    event,
                    namespace,
                    location,
                    nominalStartTime,
                    nominalEndTime,
                    log,
                    parentRun.get(),
                    uuid));

    // construct the simple name of the job by removing the parent prefix plus the dot '.' separator
    String jobName =
        parentJob
            .map(
                p -> {
                  if (event.getJob().getName().startsWith(p.getName() + '.')) {
                    return event.getJob().getName().substring(p.getName().length() + 1);
                  } else {
                    return event.getJob().getName();
                  }
                })
            .orElse(event.getJob().getName());
    log.debug(
        "Calculated job name {} from job {} with parent {}",
        jobName,
        event.getJob().getName(),
        parentJob.map(JobRow::getName));
    return parentJob
        .map(
            parent ->
                jobDao.upsertJob(
                    UUID.randomUUID(),
                    parent.getUuid(),
                    getJobType(event.getJob()),
                    now,
                    namespace.getUuid(),
                    namespace.getName(),
                    jobName,
                    description,
                    location,
                    null,
                    jobDao.toJson(toDatasetId(event.getInputs()), mapper)))
        .orElseGet(
            () ->
                jobDao.upsertJob(
                    UUID.randomUUID(),
                    getJobType(event.getJob()),
                    now,
                    namespace.getUuid(),
                    namespace.getName(),
                    jobName,
                    description,
                    location,
                    null,
                    jobDao.toJson(toDatasetId(event.getInputs()), mapper)));
  }

  private JobRow findParentJobRow(
      LineageEvent event,
      NamespaceRow namespace,
      String location,
      Instant nominalStartTime,
      Instant nominalEndTime,
      Logger log,
      ParentRunFacet facet,
      UUID uuid) {
    try {
      log.debug("Found parent run event {}", facet);
      PGobject inputs = new PGobject();
      inputs.setType("json");
      inputs.setValue("[]");
      JobRow parentJobRow =
          createRunDao()
              .findJobRowByRunUuid(uuid)
              .map(
                  j -> {
                    String parentJobName =
                        facet.getJob().getName().equals(event.getJob().getName())
                            ? Utils.parseParentJobName(facet.getJob().getName())
                            : facet.getJob().getName();
                    if (j.getNamespaceName().equals(facet.getJob().getNamespace())
                        && j.getName().equals(parentJobName)) {
                      return j;
                    } else {
                      // Addresses an Airflow integration bug that generated conflicting run UUIDs
                      // for DAGs that had the same name, but ran in different namespaces.
                      UUID parentRunUuid =
                          Utils.toNameBasedUuid(
                              facet.getJob().getNamespace(), parentJobName, uuid.toString());
                      log.warn(
                          "Parent Run id {} has a different job name '{}.{}' from facet '{}.{}'. "
                              + "Assuming Run UUID conflict and generating a new UUID {}",
                          uuid,
                          j.getNamespaceName(),
                          j.getName(),
                          facet.getJob().getNamespace(),
                          facet.getJob().getName(),
                          parentRunUuid);
                      return createParentJobRunRecord(
                          event,
                          namespace,
                          location,
                          nominalStartTime,
                          nominalEndTime,
                          parentRunUuid,
                          facet,
                          inputs);
                    }
                  })
              .orElseGet(
                  () ->
                      createParentJobRunRecord(
                          event,
                          namespace,
                          location,
                          nominalStartTime,
                          nominalEndTime,
                          uuid,
                          facet,
                          inputs));
      log.debug("Found parent job record {}", parentJobRow);
      return parentJobRow;
    } catch (Exception e) {
      throw new RuntimeException("Unable to insert parent run", e);
    }
  }

  private JobRow createParentJobRunRecord(
      LineageEvent event,
      NamespaceRow namespace,
      String location,
      Instant nominalStartTime,
      Instant nominalEndTime,
      UUID uuid,
      ParentRunFacet facet,
      PGobject inputs) {
    Instant now = event.getEventTime().withZoneSameInstant(ZoneId.of("UTC")).toInstant();
    Logger log = LoggerFactory.getLogger(OpenLineageDao.class);
    String parentJobName =
        facet.getJob().getName().equals(event.getJob().getName())
            ? Utils.parseParentJobName(facet.getJob().getName())
            : facet.getJob().getName();
    JobRow newParentJobRow =
        createJobDao()
            .upsertJob(
                UUID.randomUUID(),
                getJobType(event.getJob()),
                now,
                namespace.getUuid(),
                namespace.getName(),
                parentJobName,
                null,
                location,
                null,
                inputs);
    log.info("Created new parent job record {}", newParentJobRow);

    RunArgsRow argsRow =
        createRunArgsDao()
            .upsertRunArgs(UUID.randomUUID(), now, "{}", Utils.checksumFor(ImmutableMap.of()));

    Optional<RunState> runState = Optional.ofNullable(event.getEventType()).map(this::getRunState);
    RunDao runDao = createRunDao();
    RunRow newRow =
        runDao.upsert(
            uuid,
            null,
            facet.getRun().getRunId(),
            now,
            newParentJobRow.getUuid(),
            null,
            argsRow.getUuid(),
            nominalStartTime,
            nominalEndTime,
            runState.orElse(null),
            now,
            namespace.getName(),
            newParentJobRow.getName(),
            newParentJobRow.getLocation());
    log.info("Created new parent run record {}", newRow);

    runState
        .map(rs -> createRunStateDao().upsert(UUID.randomUUID(), now, uuid, rs))
        .ifPresent(
            runStateRow -> {
              UUID runStateUuid = runStateRow.getUuid();
              if (RunState.valueOf(runStateRow.getState()).isDone()) {
                runDao.updateEndState(uuid, now, runStateUuid);
              } else {
                runDao.updateStartState(uuid, now, runStateUuid);
              }
            });

    return newParentJobRow;
  }

  default Set<DatasetId> toDatasetId(List<Dataset> datasets) {
    Set<DatasetId> set = new HashSet<>();
    if (datasets == null) {
      return set;
    }
    for (Dataset dataset : datasets) {
      set.add(
          new DatasetId(
              NamespaceName.of(dataset.getNamespace()), DatasetName.of(dataset.getName())));
    }
    return set;
  }

  default void updateMarquezOnComplete(
      LineageEvent event, UpdateLineageRow updateLineageRow, RunState runState) {
    BagOfJobVersionInfo bagOfJobVersionInfo =
        createJobVersionDao()
            .upsertJobVersionOnRunTransition(
                updateLineageRow.getJob(),
                updateLineageRow.getRun().getUuid(),
                runState,
                event.getEventTime().toInstant());
    updateLineageRow.setJobVersionBag(bagOfJobVersionInfo);
  }

  default String getUrlOrNull(String uri) {
    try {
      return new URI(uri).toASCIIString();
    } catch (URISyntaxException e) {
      try {
        // assume host as string
        return new URI("http://" + uri).toASCIIString();
      } catch (Exception ex) {
        return null;
      }
    }
  }

  default String formatNamespaceName(String namespace) {
    return namespace.replaceAll("[^a-z:/A-Z0-9\\-_.@+]", "_");
  }

  default JobType getJobType(Job job) {
    return JobType.BATCH;
  }

  default DatasetRecord upsertLineageDataset(
      ModelDaos daos, Dataset ds, Instant now, UUID runUuid, boolean isInput) {
    NamespaceRow dsNamespace =
        daos.getNamespaceDao()
            .upsertNamespaceRow(UUID.randomUUID(), now, ds.getNamespace(), DEFAULT_NAMESPACE_OWNER);

    SourceRow source;
    if (ds.getFacets() != null && ds.getFacets().getDataSource() != null) {
      source =
          daos.getSourceDao()
              .upsert(
                  UUID.randomUUID(),
                  getSourceType(ds),
                  now,
                  ds.getFacets().getDataSource().getName(),
                  getUrlOrNull(ds.getFacets().getDataSource().getUri()));
    } else {
      source =
          daos.getSourceDao()
              .upsertOrDefault(UUID.randomUUID(), getSourceType(ds), now, DEFAULT_SOURCE_NAME, "");
    }

    String dsDescription = null;
    if (ds.getFacets() != null && ds.getFacets().getDocumentation() != null) {
      dsDescription = ds.getFacets().getDocumentation().getDescription();
    }

    NamespaceRow datasetNamespace =
        daos.getNamespaceDao()
            .upsertNamespaceRow(
                UUID.randomUUID(),
                now,
                formatNamespaceName(ds.getNamespace()),
                DEFAULT_NAMESPACE_OWNER);

    DatasetSymlinkRow symlink =
        daos.getDatasetSymlinkDao()
            .upsertDatasetSymlinkRow(
                UUID.randomUUID(),
                formatDatasetName(ds.getName()),
                dsNamespace.getUuid(),
                true,
                null,
                now);

    Optional.ofNullable(ds.getFacets())
        .map(facets -> facets.getSymlinks())
        .ifPresent(
            el ->
                el.getIdentifiers().stream()
                    .forEach(
                        id ->
                            daos.getDatasetSymlinkDao()
                                .doUpsertDatasetSymlinkRow(
                                    symlink.getUuid(),
                                    id.getName(),
                                    daos.getNamespaceDao()
                                        .upsertNamespaceRow(
                                            UUID.randomUUID(),
                                            now,
                                            id.getNamespace(),
                                            DEFAULT_NAMESPACE_OWNER)
                                        .getUuid(),
                                    false,
                                    id.getType(),
                                    now)));
    String dslifecycleState =
        Optional.ofNullable(ds.getFacets())
            .map(DatasetFacets::getLifecycleStateChange)
            .map(LifecycleStateChangeFacet::getLifecycleStateChange)
            .orElse("");

    DatasetRow datasetRow =
        daos.getDatasetDao()
            .upsert(
                symlink.getUuid(),
                getDatasetType(ds),
                now,
                datasetNamespace.getUuid(),
                datasetNamespace.getName(),
                source.getUuid(),
                source.getName(),
                formatDatasetName(ds.getName()),
                ds.getName(),
                dsDescription,
                dslifecycleState.equalsIgnoreCase("DROP"));

    List<SchemaField> fields =
        Optional.ofNullable(ds.getFacets())
            .map(DatasetFacets::getSchema)
            .map(SchemaDatasetFacet::getFields)
            .orElse(null);

    final DatasetRow dsRow = datasetRow;
    DatasetVersionRow datasetVersionRow =
        datasetRow
            .getCurrentVersionUuid()
            .filter(v -> isInput) // only fetch the current version if this is a read
            .flatMap(daos.getDatasetVersionDao()::findRowByUuid)
            // if this is a write _or_ if the dataset has no current version,
            // create a new version
            .orElseGet(
                () -> {
                  UUID versionUuid =
                      Utils.newDatasetVersionFor(
                              dsNamespace.getName(),
                              source.getName(),
                              dsRow.getPhysicalName(),
                              symlink.getName(),
                              dslifecycleState,
                              fields,
                              runUuid)
                          .getValue();
                  DatasetVersionRow row =
                      daos.getDatasetVersionDao()
                          .upsert(
                              UUID.randomUUID(),
                              now,
                              dsRow.getUuid(),
                              versionUuid,
                              isInput ? null : runUuid,
                              daos.getDatasetVersionDao().toPgObjectSchemaFields(fields),
                              dsNamespace.getName(),
                              ds.getName(),
                              dslifecycleState);
                  return row;
                });
    List<DatasetFieldMapping> datasetFieldMappings = new ArrayList<>();
    List<DatasetFieldRow> datasetFields = new ArrayList<>();
    if (fields != null) {
      for (SchemaField field : fields) {
        DatasetFieldRow datasetFieldRow =
            daos.getDatasetFieldDao()
                .upsert(
                    UUID.randomUUID(),
                    now,
                    field.getName(),
                    field.getType(),
                    field.getDescription(),
                    datasetRow.getUuid());
        datasetFields.add(datasetFieldRow);
        datasetFieldMappings.add(
            new DatasetFieldMapping(datasetVersionRow.getUuid(), datasetFieldRow.getUuid()));
      }
    }
    daos.getDatasetFieldDao().updateFieldMapping(datasetFieldMappings);

    if (isInput) {
      daos.getRunDao().updateInputMapping(runUuid, datasetVersionRow.getUuid());

      // TODO - this is a short term fix until
      // https://github.com/MarquezProject/marquez/issues/1361
      // is fully thought out
      if (datasetRow.getCurrentVersionUuid().isEmpty()) {
        daos.getDatasetDao().updateVersion(dsRow.getUuid(), now, datasetVersionRow.getUuid());
        datasetRow = datasetRow.withCurrentVersionUuid(datasetVersionRow.getUuid());
      }
    }

    List<ColumnLineageRow> columnLineageRows = Collections.emptyList();
    if (!isInput) {
      columnLineageRows =
          upsertColumnLineage(runUuid, ds, now, datasetFields, datasetVersionRow, daos);
    }

    return new DatasetRecord(datasetRow, datasetVersionRow, datasetNamespace, columnLineageRows);
  }

  private List<ColumnLineageRow> upsertColumnLineage(
      UUID runUuid,
      Dataset ds,
      Instant now,
      List<DatasetFieldRow> datasetFields,
      DatasetVersionRow datasetVersionRow,
      ModelDaos daos) {
    Logger log = LoggerFactory.getLogger(OpenLineageDao.class);

    // get all the fields related to this particular run
    List<InputFieldData> runFields =
        daos.getDatasetFieldDao().findInputFieldsDataAssociatedWithRun(runUuid);
    log.debug("Found input datasets fields for run '{}': {}", runUuid, runFields);

    return Optional.ofNullable(ds.getFacets())
        .map(DatasetFacets::getColumnLineage)
        .map(LineageEvent.ColumnLineageDatasetFacet::getFields)
        .map(LineageEvent.ColumnLineageDatasetFacetFields::getAdditional)
        .stream()
        .flatMap(map -> map.keySet().stream())
        .filter(
            columnName ->
                ds.getFacets().getColumnLineage().getFields().getAdditional().get(columnName)
                    instanceof LineageEvent.ColumnLineageOutputColumn)
        .flatMap(
            columnName -> {
              LineageEvent.ColumnLineageOutputColumn columnLineage =
                  ds.getFacets().getColumnLineage().getFields().getAdditional().get(columnName);
              Optional<DatasetFieldRow> outputField =
                  datasetFields.stream().filter(dfr -> dfr.getName().equals(columnName)).findAny();

              if (outputField.isEmpty()) {
                log.error(
                    "Cannot produce column lineage for missing output field in output dataset: {}",
                    columnName);
                return Stream.empty();
              }

              // get field uuids of input columns related to this run
              List<Pair<UUID, UUID>> inputFields =
                  runFields.stream()
                      .filter(
                          fieldData ->
                              columnLineage.getInputFields().stream()
                                  .filter(
                                      of ->
                                          of.getNamespace().equals(fieldData.getNamespace())
                                              && of.getName().equals(fieldData.getDatasetName())
                                              && of.getField().equals(fieldData.getField()))
                                  .findAny()
                                  .isPresent())
                      .map(
                          fieldData ->
                              Pair.of(
                                  fieldData.getDatasetVersionUuid(),
                                  fieldData.getDatasetFieldUuid()))
                      .collect(Collectors.toList());

              log.debug(
                  "Adding column lineage on output field '{}' for dataset version '{}' with input fields: {}",
                  outputField.get().getName(),
                  datasetVersionRow.getUuid(),
                  inputFields);
              return daos
                  .getColumnLineageDao()
                  .upsertColumnLineageRow(
                      datasetVersionRow.getUuid(),
                      outputField.get().getUuid(),
                      inputFields,
                      columnLineage.getTransformationDescription(),
                      columnLineage.getTransformationType(),
                      now)
                  .stream();
            })
        .collect(Collectors.toList());
  }

  default String formatDatasetName(String name) {
    return name;
  }

  default String getSourceType(Dataset ds) {
    return SourceType.of("POSTGRESQL").getValue();
  }

  default DatasetType getDatasetType(Dataset ds) {
    return DatasetType.DB_TABLE;
  }

  default RunState getRunState(String eventType) {
    if (eventType == null) {
      return RunState.RUNNING;
    }
    switch (eventType.toLowerCase()) {
      case "complete":
        return RunState.COMPLETED;
      case "abort":
        return RunState.ABORTED;
      case "fail":
        return RunState.FAILED;
      case "start":
        return RunState.RUNNING;
      default:
        return RunState.RUNNING;
    }
  }

  default Map<String, String> createRunArgs(LineageEvent event) {
    Map<String, String> args = new LinkedHashMap<>();
    if (event.getRun().getFacets() != null) {
      if (event.getRun().getFacets().getNominalTime() != null) {
        args.put(
            "nominal_start_time",
            event.getRun().getFacets().getNominalTime().getNominalStartTime().toString());
        if (event.getRun().getFacets().getNominalTime().getNominalEndTime() != null) {
          args.put(
              "nominal_end_time",
              event.getRun().getFacets().getNominalTime().getNominalEndTime().toString());
        }
      }
      if (event.getRun().getFacets().getParent() != null) {
        args.put("run_id", event.getRun().getFacets().getParent().getRun().getRunId());
        args.put("name", event.getRun().getFacets().getParent().getJob().getName());
        args.put("namespace", event.getRun().getFacets().getParent().getJob().getNamespace());
      }
    }
    return args;
  }

  default UUID runToUuid(String runId) {
    try {
      return UUID.fromString(runId);
    } catch (Exception e) {
      // Allow non-UUID runId
      return UUID.nameUUIDFromBytes(runId.getBytes());
    }
  }

  default PGobject createJsonArray(BaseEvent event, ObjectMapper mapper) {
    try {
      PGobject jsonObject = new PGobject();
      jsonObject.setType("json");
      jsonObject.setValue(mapper.writeValueAsString(event));
      return jsonObject;
    } catch (Exception e) {
      throw new RuntimeException("Could write lineage event to db", e);
    }
  }
}
