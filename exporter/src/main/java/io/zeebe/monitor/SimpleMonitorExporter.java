/*
 * Copyright © 2017 camunda services GmbH (info@camunda.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.zeebe.monitor;

import io.zeebe.exporter.context.Context;
import io.zeebe.exporter.context.Controller;
import io.zeebe.exporter.record.Record;
import io.zeebe.exporter.record.value.DeploymentRecordValue;
import io.zeebe.exporter.record.value.IncidentRecordValue;
import io.zeebe.exporter.record.value.WorkflowInstanceRecordValue;
import io.zeebe.exporter.record.value.deployment.DeployedWorkflow;
import io.zeebe.exporter.record.value.deployment.DeploymentResource;
import io.zeebe.exporter.spi.Exporter;
import io.zeebe.protocol.clientapi.RecordType;
import io.zeebe.protocol.clientapi.ValueType;
import io.zeebe.protocol.intent.DeploymentIntent;
import io.zeebe.protocol.intent.Intent;
import io.zeebe.protocol.intent.WorkflowInstanceIntent;
import org.slf4j.Logger;

import java.net.URI;
import java.net.URL;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.*;
import java.util.function.Consumer;
import java.util.stream.Collectors;

public class SimpleMonitorExporter implements Exporter {

  private static final String INSERT_WORKFLOW =
      "INSERT INTO WORKFLOW (ID_, KEY_, BPMN_PROCESS_ID_, VERSION_, RESOURCE_, TIMESTAMP_) VALUES ('%s', %d, '%s', %d, '%s', %d);";

  private static final String INSERT_WORKFLOW_INSTANCE =
      "INSERT INTO WORKFLOW_INSTANCE"
          + " (ID_, PARTITION_ID_, KEY_, BPMN_PROCESS_ID_, VERSION_, WORKFLOW_KEY_, START_)"
          + " VALUES "
          + "('%s', %d, %d, '%s', %d, %d, %d);";

  private static final String UPDATE_WORKFLOW_INSTANCE =
      "UPDATE WORKFLOW_INSTANCE SET END_ = %d WHERE KEY_ = %d;";

  private static final String INSERT_ACTIVITY_INSTANCE =
      "INSERT INTO ACTIVITY_INSTANCE"
          + " (ID_, PARTITION_ID_, KEY_, INTENT_, WORKFLOW_INSTANCE_KEY_, ACTIVITY_ID_, SCOPE_INSTANCE_KEY_, PAYLOAD_, WORKFLOW_KEY_, TIMESTAMP_)"
          + " VALUES "
          + "('%s', %d, %d, '%s', %d, '%s', %d, '%s', %d, %d);";

  private static final String INSERT_INCIDENT =
      "INSERT INTO INCIDENT"
          + " (ID_, KEY_, INTENT_, WORKFLOW_INSTANCE_KEY_, ACTIVITY_INSTANCE_KEY_, JOB_KEY_, ERROR_TYPE_, ERROR_MSG_, TIMESTAMP_)"
          + " VALUES "
          + "('%s', %d, '%s', %d, %d, %d, '%s', '%s', %d)";

  public static final String CREATE_SCHEMA_SQL_PATH = "/CREATE_SCHEMA.sql";

  private final Map<ValueType, Consumer<Record>> insertCreatorPerType = new HashMap<>();
  private final List<String> sqlStatements;

  private Logger log;
  private Controller controller;
  private SimpleMonitorExporterConfiguration configuration;

  private Connection connection;
  private int batchSize;
  private int batchTimerMilli;
  private Duration batchExecutionTimer;
  private long lastPosition;

  public SimpleMonitorExporter() {
    insertCreatorPerType.put(ValueType.DEPLOYMENT, this::exportDeploymentRecord);
    insertCreatorPerType.put(ValueType.WORKFLOW_INSTANCE, this::exportWorkflowInstanceRecord);
    insertCreatorPerType.put(ValueType.INCIDENT, this::exportIncidentRecord);

    sqlStatements = new ArrayList<>();
  }

  @Override
  public void configure(final Context context) {
    log = context.getLogger();
    configuration =
        context.getConfiguration().instantiate(SimpleMonitorExporterConfiguration.class);
    batchSize = configuration.batchSize;
    batchTimerMilli = configuration.batchTimerMilli;

    log.debug("Exporter configured with {}", configuration);
    try {
      Class.forName(configuration.driverName);
    } catch (final ClassNotFoundException e) {
      throw new RuntimeException("Driver not found in class path", e);
    }
  }

  @Override
  public void open(final Controller controller) {
    try {
      connection =
          DriverManager.getConnection(
              configuration.jdbcUrl, configuration.userName, configuration.password);
      connection.setAutoCommit(true);
    } catch (final SQLException e) {
      throw new RuntimeException("Error on opening database.", e);
    }

    createTables();
    log.info("Start exporting to {}.", configuration.jdbcUrl);

    this.controller = controller;
    if (batchTimerMilli > 0) {
      batchExecutionTimer = Duration.ofMillis(batchTimerMilli);
      this.controller.scheduleTask(batchExecutionTimer, this::batchTimerExecution);
    }
  }

  private void createTables() {
    try (final Statement statement = connection.createStatement()) {

      final URL resource = SimpleMonitorExporter.class.getResource(CREATE_SCHEMA_SQL_PATH);
      final URI uri = resource.toURI();
      if (resource.getFile().contains("!")) {
        FileSystems.newFileSystem(uri, Collections.EMPTY_MAP);
      }
      final Path path = Paths.get(uri);
      final byte[] bytes = Files.readAllBytes(path);
      final String sql = new String(bytes);

      log.info("Create tables:\n{}", sql);
      statement.executeUpdate(sql);
    } catch (final Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void close() {
    try {
      connection.close();
    } catch (final Exception e) {
      log.warn("Failed to close jdbc connection", e);
    }
    log.info("Exporter closed");
  }

  @Override
  public void export(final Record record) {
    lastPosition = record.getPosition();
    if (record.getMetadata().getRecordType() != RecordType.EVENT) {
      return;
    }

    final Consumer<Record> recordConsumer =
        insertCreatorPerType.get(record.getMetadata().getValueType());
    if (recordConsumer != null) {
      recordConsumer.accept(record);

      if (sqlStatements.size() > batchSize) {
        executeSqlStatementBatch();
      }
    }
  }

  private void batchTimerExecution() {
    executeSqlStatementBatch();
    controller.scheduleTask(batchExecutionTimer, this::batchTimerExecution);
  }

  private void executeSqlStatementBatch() {
    try (final Statement statement = connection.createStatement()) {
      for (final String insert : sqlStatements) {
        statement.addBatch(insert);
      }
      statement.executeBatch();
      sqlStatements.clear();
    } catch (final Exception e) {
      log.error("Batch insert failed!", e);
    }
    controller.updateLastExportedRecordPosition(lastPosition);
  }

  private void exportDeploymentRecord(final Record record) {
    if (DeploymentIntent.CREATED != record.getMetadata().getIntent()) {
      return;
    }
    final long timestamp = record.getTimestamp().toEpochMilli();
    final DeploymentRecordValue deploymentRecordValue = (DeploymentRecordValue) record.getValue();

    final List<DeploymentResource> resources = deploymentRecordValue.getResources();
    for (final DeploymentResource resource : resources) {
      final List<DeployedWorkflow> deployedWorkflows =
          deploymentRecordValue
              .getDeployedWorkflows()
              .stream()
              .filter(w -> w.getResourceName().equals(resource.getResourceName()))
              .collect(Collectors.toList());
      for (final DeployedWorkflow deployedWorkflow : deployedWorkflows) {
        final String insertStatement =
            String.format(
                INSERT_WORKFLOW,
                createId(),
                deployedWorkflow.getWorkflowKey(),
                getCleanString(deployedWorkflow.getBpmnProcessId()),
                deployedWorkflow.getVersion(),
                getCleanString(new String(resource.getResource())),
                timestamp);
        sqlStatements.add(insertStatement);
      }
    }
  }

  private boolean isWorkflowInstance(
      final Record record, final WorkflowInstanceRecordValue workflowInstanceRecordValue) {
    return workflowInstanceRecordValue.getWorkflowInstanceKey() == record.getKey();
  }

  private void exportWorkflowInstanceRecord(final Record record) {
    final long key = record.getKey();
    final int partitionId = record.getMetadata().getPartitionId();
    final Intent intent = record.getMetadata().getIntent();
    final long timestamp = record.getTimestamp().toEpochMilli();

    final WorkflowInstanceRecordValue workflowInstanceRecordValue =
        (WorkflowInstanceRecordValue) record.getValue();

    if (isWorkflowInstance(record, workflowInstanceRecordValue)) {
      exportWorkflowInstance(key, partitionId, intent, timestamp, workflowInstanceRecordValue);
    } else {
      exportActivityInstance(key, partitionId, intent, timestamp, workflowInstanceRecordValue);
    }
  }

  private void exportWorkflowInstance(
      final long key,
      final int partitionId,
      final Intent intent,
      final long timestamp,
      final WorkflowInstanceRecordValue workflowInstanceRecordValue) {
    final boolean wasWorkflowInstanceStarted = intent == WorkflowInstanceIntent.CREATED;
    final boolean wasWorkflowInstanceEnded =
        intent == WorkflowInstanceIntent.ELEMENT_TERMINATED
            || intent == WorkflowInstanceIntent.CREATED;

    if (wasWorkflowInstanceStarted) {
      final String bpmnProcessId = getCleanString(workflowInstanceRecordValue.getBpmnProcessId());
      final int version = workflowInstanceRecordValue.getVersion();
      final long workflowKey = workflowInstanceRecordValue.getWorkflowKey();

      final String insertWorkflowInstanceStatement =
          String.format(
              INSERT_WORKFLOW_INSTANCE,
              createId(),
              partitionId,
              key,
              bpmnProcessId,
              version,
              workflowKey,
              timestamp);
      sqlStatements.add(insertWorkflowInstanceStatement);
    } else if (wasWorkflowInstanceEnded) {
      final String updateWorkflowInstanceStatement =
          String.format(UPDATE_WORKFLOW_INSTANCE, timestamp, key);
      sqlStatements.add(updateWorkflowInstanceStatement);
    }
  }

  private void exportActivityInstance(
      final long key,
      final int partitionId,
      final Intent intent,
      final long timestamp,
      final WorkflowInstanceRecordValue workflowInstanceRecordValue) {
    final long workflowInstanceKey = workflowInstanceRecordValue.getWorkflowInstanceKey();
    final String activityId = getCleanString(workflowInstanceRecordValue.getActivityId());
    final long scopeInstanceKey = workflowInstanceRecordValue.getScopeInstanceKey();
    final String payload = getCleanString(workflowInstanceRecordValue.getPayload());
    final long workflowKey = workflowInstanceRecordValue.getWorkflowKey();

    final String insertActivityInstanceStatement =
        String.format(
            INSERT_ACTIVITY_INSTANCE,
            createId(),
            partitionId,
            key,
            intent,
            workflowInstanceKey,
            activityId,
            scopeInstanceKey,
            payload,
            workflowKey,
            timestamp);
    sqlStatements.add(insertActivityInstanceStatement);
  }

  private void exportIncidentRecord(final Record record) {
    final long key = record.getKey();
    final String intent = record.getMetadata().getIntent().name();
    final long timestamp = record.getTimestamp().toEpochMilli();

    final IncidentRecordValue incidentRecordValue = (IncidentRecordValue) record.getValue();
    final long workflowInstanceKey = incidentRecordValue.getWorkflowInstanceKey();
    final long activityInstanceKey = incidentRecordValue.getActivityInstanceKey();
    final long jobKey = incidentRecordValue.getJobKey();
    final String errorType = getCleanString(incidentRecordValue.getErrorType());
    final String errorMessage = getCleanString(incidentRecordValue.getErrorMessage());

    final String insertStatement =
        String.format(
            INSERT_INCIDENT,
            createId(),
            key,
            intent,
            workflowInstanceKey,
            activityInstanceKey,
            jobKey,
            errorType,
            errorMessage,
            timestamp);
    sqlStatements.add(insertStatement);
  }

  private String getCleanString(final String string) {
    return string.trim().replaceAll("'", "`");
  }

  private String createId() {
    return UUID.randomUUID().toString();
  }
}
