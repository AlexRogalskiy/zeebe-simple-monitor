package io.zeebe.monitor.rest;

import io.zeebe.monitor.entity.ActivityInstanceEntity;
import io.zeebe.monitor.entity.IncidentEntity;
import io.zeebe.monitor.entity.WorkflowEntity;
import io.zeebe.monitor.entity.WorkflowInstanceEntity;
import io.zeebe.monitor.repository.ActivityInstanceRepository;
import io.zeebe.monitor.repository.IncidentRepository;
import io.zeebe.monitor.repository.WorkflowInstanceRepository;
import io.zeebe.monitor.repository.WorkflowRepository;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Pageable;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;

@Controller
public class ViewController {

  private static final List<String> WORKFLOW_INSTANCE_ENTERED_INTENTS =
      Arrays.asList(
          "ELEMENT_ACTIVATED", "START_EVENT_OCCURRED", "END_EVENT_OCCURRED", "GATEWAY_ACTIVATED");

  private static final List<String> WORKFLOW_INSTANCE_COMPLETED_INTENTS =
      Arrays.asList(
          "ELEMENT_COMPLETED",
          "ELEMENT_TERMINATED",
          "START_EVENT_OCCURRED",
          "END_EVENT_OCCURRED",
          "GATEWAY_ACTIVATED");

  @Autowired private WorkflowRepository workflowRepository;

  @Autowired private WorkflowInstanceRepository workflowInstanceRepository;

  @Autowired private ActivityInstanceRepository activityInstanceRepository;

  @Autowired private IncidentRepository incidentRepository;

  @GetMapping("/")
  public String index(Map<String, Object> model, Pageable pageable) {
    return workflowList(model, pageable);
  }

  @GetMapping("/views/workflows")
  public String workflowList(Map<String, Object> model, Pageable pageable) {

    final long count = workflowRepository.count();

    final List<WorkflowDto> workflows = new ArrayList<>();
    for (WorkflowEntity workflowEntity : workflowRepository.findAll(pageable)) {
      final WorkflowDto dto = toDto(workflowEntity);
      workflows.add(dto);
    }

    model.put("workflows", workflows);
    model.put("count", count);

    addPaginationToModel(model, pageable, count);

    return "workflow-list-view";
  }

  private WorkflowDto toDto(WorkflowEntity workflowEntity) {
    final long workflowKey = workflowEntity.getKey();

    final long running = workflowInstanceRepository.countByWorkflowKeyAndEndIsNull(workflowKey);
    final long ended = workflowInstanceRepository.countByWorkflowKeyAndEndIsNotNull(workflowKey);

    final WorkflowDto dto = WorkflowDto.from(workflowEntity, running, ended);
    return dto;
  }

  @GetMapping("/views/workflows/{key}")
  public String workflowDetail(
      @PathVariable long key, Map<String, Object> model, Pageable pageable) {

    workflowRepository
        .findByKey(key)
        .ifPresent(
            workflow -> {
              model.put("workflow", toDto(workflow));
              model.put("resource", workflow.getResource());
            });

    final long count = workflowInstanceRepository.countByWorkflowKey(key);

    final List<WorkflowInstanceListDto> instances = new ArrayList<>();
    for (WorkflowInstanceEntity instanceEntity :
        workflowInstanceRepository.findByWorkflowKey(key, pageable)) {
      instances.add(toDto(instanceEntity));
    }

    model.put("instances", instances);
    model.put("count", count);

    addPaginationToModel(model, pageable, count);

    return "workflow-detail-view";
  }

  private WorkflowInstanceListDto toDto(WorkflowInstanceEntity instance) {

    final WorkflowInstanceListDto dto = new WorkflowInstanceListDto();
    dto.setWorkflowInstanceKey(instance.getKey());

    dto.setBpmnProcessId(instance.getBpmnProcessId());
    dto.setWorkflowKey(instance.getWorkflowKey());

    final boolean isEnded = instance.getEnd() != null && instance.getEnd() > 0;
    dto.setState(isEnded ? "Ended" : "Running");

    dto.setStartTime(Instant.ofEpochMilli(instance.getStart()).toString());

    if (isEnded) {
      dto.setEndTime(Instant.ofEpochMilli(instance.getEnd()).toString());
    }

    return dto;
  }

  @GetMapping("/views/instances")
  public String instanceList(Map<String, Object> model, Pageable pageable) {

    final long count = workflowInstanceRepository.count();

    final List<WorkflowInstanceListDto> instances = new ArrayList<>();
    for (WorkflowInstanceEntity instanceEntity : workflowInstanceRepository.findAll(pageable)) {
      final WorkflowInstanceListDto dto = toDto(instanceEntity);
      instances.add(dto);
    }

    model.put("instances", instances);
    model.put("count", count);

    addPaginationToModel(model, pageable, count);

    return "instance-list-view";
  }

  @GetMapping("/views/instances/{key}")
  public String instanceDetail(
      @PathVariable long key, Map<String, Object> model, Pageable pageable) {

    workflowInstanceRepository
        .findByKey(key)
        .ifPresent(
            instance -> {
              workflowRepository
                  .findByKey(instance.getWorkflowKey())
                  .ifPresent(workflow -> model.put("resource", workflow.getResource()));

              model.put("instance", toInstanceDto(instance));
            });

    return "instance-detail-view";
  }

  private WorkflowInstanceDto toInstanceDto(WorkflowInstanceEntity instance) {
    final List<ActivityInstanceEntity> events =
        StreamSupport.stream(
                activityInstanceRepository
                    .findByWorkflowInstanceKey(instance.getKey())
                    .spliterator(),
                false)
            .collect(Collectors.toList());

    final ActivityInstanceEntity lastEvent = events.get(events.size() - 1);

    final WorkflowInstanceDto dto = new WorkflowInstanceDto();
    dto.setWorkflowInstanceKey(instance.getKey());

    dto.setPartitionId(instance.getPartitionId());

    dto.setWorkflowKey(instance.getWorkflowKey());
    dto.setPayload(lastEvent.getPayload());

    dto.setBpmnProcessId(instance.getBpmnProcessId());
    dto.setVersion(instance.getVersion());

    final boolean isEnded = instance.getEnd() != null && instance.getEnd() > 0;
    dto.setState(isEnded ? "Ended" : "Running");
    dto.setRunning(!isEnded);

    dto.setStartTime(Instant.ofEpochMilli(instance.getStart()).toString());

    if (isEnded) {
      dto.setEndTime(Instant.ofEpochMilli(instance.getEnd()).toString());
    }

    final List<String> completedActivities =
        events
            .stream()
            .filter(e -> WORKFLOW_INSTANCE_COMPLETED_INTENTS.contains(e.getIntent()))
            .map(ActivityInstanceEntity::getActivityId)
            .collect(Collectors.toList());

    final List<String> activeActivities =
        events
            .stream()
            .filter(e -> WORKFLOW_INSTANCE_ENTERED_INTENTS.contains(e.getIntent()))
            .map(ActivityInstanceEntity::getActivityId)
            .filter(id -> !completedActivities.contains(id))
            .collect(Collectors.toList());
    dto.setActiveActivities(activeActivities);

    final List<String> takenSequenceFlows =
        events
            .stream()
            .filter(e -> e.getIntent().equals("SEQUENCE_FLOW_TAKEN"))
            .map(ActivityInstanceEntity::getActivityId)
            .collect(Collectors.toList());
    dto.setTakenSequenceFlows(takenSequenceFlows);

    final Map<String, Long> completedElementsById =
        events
            .stream()
            .filter(e -> WORKFLOW_INSTANCE_COMPLETED_INTENTS.contains(e.getIntent()))
            .collect(
                Collectors.groupingBy(
                    ActivityInstanceEntity::getActivityId, Collectors.counting()));

    final Map<String, Long> enteredElementsById =
        events
            .stream()
            .filter(e -> WORKFLOW_INSTANCE_ENTERED_INTENTS.contains(e.getIntent()))
            .collect(
                Collectors.groupingBy(
                    ActivityInstanceEntity::getActivityId, Collectors.counting()));

    final List<ElementInstanceState> elementStates =
        enteredElementsById
            .entrySet()
            .stream()
            .map(
                e -> {
                  final String elementId = e.getKey();

                  final long enteredInstances = e.getValue();
                  final long completedInstances = completedElementsById.getOrDefault(elementId, 0L);

                  final ElementInstanceState state = new ElementInstanceState();
                  state.setElementId(elementId);
                  state.setActiveInstances(enteredInstances - completedInstances);
                  state.setEndedInstances(completedInstances);

                  return state;
                })
            .collect(Collectors.toList());

    dto.setElementInstances(elementStates);

    final List<AuditLogEntry> auditLogEntries =
        events
            .stream()
            .map(
                e -> {
                  final AuditLogEntry entry = new AuditLogEntry();

                  entry.setKey(e.getKey());
                  entry.setScopeInstanceKey(e.getScopeInstanceKey());
                  entry.setElementId(e.getActivityId());
                  entry.setPaylaod(e.getPayload());
                  entry.setState(e.getIntent());
                  entry.setTimestamp(Instant.ofEpochMilli(e.getTimestamp()).toString());

                  return entry;
                })
            .collect(Collectors.toList());

    dto.setAuditLogEntries(auditLogEntries);

    final List<IncidentEntity> incidents =
        StreamSupport.stream(
                incidentRepository.findByWorkflowInstanceKey(instance.getKey()).spliterator(),
                false)
            .collect(Collectors.toList());

    incidents
        .stream()
        .collect(Collectors.groupingBy(IncidentEntity::getIncidentKey))
        .entrySet()
        .stream()
        .forEach(
            i -> {
              final Long incidentKey = i.getKey();

              final List<IncidentEntity> incidentEvents = i.getValue();
              final IncidentEntity lastIncidentEvent =
                  incidentEvents.get(incidentEvents.size() - 1);

              final IncidentDto incidentDto = new IncidentDto();
              incidentDto.setKey(incidentKey);

              events
                  .stream()
                  .filter(e -> e.getKey() == lastIncidentEvent.getActivityInstanceKey())
                  .findFirst()
                  .ifPresent(
                      e -> {
                        incidentDto.setActivityId(e.getActivityId());
                      });

              incidentDto.setActivityInstanceKey(lastIncidentEvent.getActivityInstanceKey());
              incidentDto.setJobKey(lastIncidentEvent.getJobKey());
              incidentDto.setErrorType(lastIncidentEvent.getErrorType());
              incidentDto.setErrorMessage(lastIncidentEvent.getErrorMessage());

              incidentDto.setState(lastIncidentEvent.getIntent());
              incidentDto.setTime(
                  Instant.ofEpochMilli(lastIncidentEvent.getTimestamp()).toString());

              dto.getIncidents().add(incidentDto);
            });

    final List<String> activitiesWitIncidents =
        incidents
            .stream()
            .collect(Collectors.groupingBy(IncidentEntity::getActivityInstanceKey))
            .entrySet()
            .stream()
            .filter(
                i -> {
                  final List<IncidentEntity> incidentEvents = i.getValue();
                  final IncidentEntity lastIncidentEvent =
                      incidentEvents.get(incidentEvents.size() - 1);

                  return lastIncidentEvent.getIntent().equals("CREATED");
                })
            .map(
                i -> {
                  return events
                      .stream()
                      .filter(e -> e.getKey() == i.getKey())
                      .findFirst()
                      .map(ActivityInstanceEntity::getActivityId)
                      .orElse("");
                })
            .distinct()
            .collect(Collectors.toList());

    dto.setIncidentActivities(activitiesWitIncidents);

    activeActivities.removeAll(activitiesWitIncidents);
    dto.setActiveActivities(activeActivities);

    return dto;
  }

  private void addPaginationToModel(
      Map<String, Object> model, Pageable pageable, final long count) {

    final int currentPage = pageable.getPageNumber();
    model.put("page", currentPage + 1);
    if (currentPage > 0) {
      model.put("prevPage", currentPage - 1);
    }
    if (count > (1 + currentPage) * pageable.getPageSize()) {
      model.put("nextPage", currentPage + 1);
    }
  }
}
