package org.example.api.executor;

import lombok.NonNull;
import org.example.api.dtos.*;
import org.example.utils.DependencyCountMap;

import java.util.*;

/// A CloudSim implemented local workflow executor.
public class LocalWorkflowExecutor implements WorkflowExecutor {
    private final Map<WorkflowTaskId, TaskDto> taskMap = new HashMap<>();
    private final Map<WorkflowTaskId, VmId> scheduledMap = new HashMap<>();
    private final Set<WorkflowTaskId> executingTasks = new HashSet<>(); //正在执行的任务集合
    private final DependencyCountMap<WorkflowTaskId> pendingDependencies = new DependencyCountMap<>(); //跟踪每个任务尚未满足的前驱数量（用于判断任务何时可以运行）
    // 每个VmId对应一个任务队列，队列里是等待这个VM的任务
    private final Map<VmId, Queue<WorkflowTaskId>> pendingTaskQueueByVm = new HashMap<>(); // Tasks that are pending for VM
    private final Map<VmId, WorkflowTaskId> readyTaskByVm = new HashMap<>(); // Tasks that are ready to be executed in VM
    // 每个VmId对应一个当前就绪且可以立即在该VM上执行的单个任务（设计上每台VM同一时刻只有一个ready任务的引用）
    
    
    @Override //有新虚拟机，把虚拟机id放在pendingTaskQueueByVm，值是一个空队列
    public void notifyNewVm(@NonNull VmDto newVm) {
        pendingTaskQueueByVm.put(new VmId(newVm.getId()), new LinkedList<>());
    }

//有新工作流，把（工作流任务id，任务DTO）放在taskMap,把子任务的前驱加一；
    @Override
    public void notifyNewWorkflow(@NonNull WorkflowDto newWorkflow) {
        // Submit to map for tasks for easy access.
        // Also create a map for holding number of pending dependencies for each task.
        for (var task : newWorkflow.getTasks()) {
            var taskId = new WorkflowTaskId(newWorkflow.getId(), task.getId());
            taskMap.put(taskId, task);

            // Add dependency from parent task
            for (var childId : task.getChildIds()) {
                var childTaskId = new WorkflowTaskId(newWorkflow.getId(), childId);
                pendingDependencies.addNewDependency(childTaskId);
            }
        }
    }

    @Override //将一个已被调度的任务 ID 入队到该 VM 的待处理任务队列
    public void notifyScheduling(@NonNull VmAssignmentDto assignment) {
        var vmId = new VmId(assignment.getVmId());
        var workflowTaskId = new WorkflowTaskId(assignment.getWorkflowId(), assignment.getTaskId());
        scheduledMap.put(workflowTaskId, vmId);

        // Move the task to the pending queue
        var pendingQueue = pendingTaskQueueByVm.get(vmId);
        pendingQueue.add(workflowTaskId);

        updateReadyTasks();
    }

    @Override //任务完成——从子任务的前驱中移除它，并从正在执行的列表中删除它
    public void notifyCompletion(int workflowId, int taskId) {
        var workflowTaskId = new WorkflowTaskId(workflowId, taskId);
        var vmId = scheduledMap.get(workflowTaskId);

        // Update tasks that were dependent on the task that just completed
        var task = taskMap.get(workflowTaskId);
        for (var childId : task.getChildIds()) {
            var childTaskId = new WorkflowTaskId(workflowId, childId);
            pendingDependencies.removeOneDependency(childTaskId);
        }

        // Remove the task from the executing list
        readyTaskByVm.remove(vmId);
        executingTasks.remove(workflowTaskId);

        updateReadyTasks();
    }

    @Override //将每台vm映射到一个就绪任务
    public List<TaskAssignmentDto> pollTaskAssignments() {
        var assignments = new ArrayList<TaskAssignmentDto>();

        // Find the VMs that are ready to execute
        for (var vmId : readyTaskByVm.keySet()) {
            var taskId = readyTaskByVm.get(vmId); //对每个vmid取出taskId
            if (!executingTasks.contains(taskId)) {
                executingTasks.add(taskId); //标记该任务为正在执行
                //创建任务分配DTO
                var assignment = new TaskAssignmentDto(taskId.workflowId(), taskId.taskId(), vmId.vmId());
                assignments.add(assignment);
            }
        }

        return assignments;
    }

    /// Updates the ready list based on the pending queues and dependencies.
    /// Only needs to call if the ready list changes or dependencies get removed.
    private void updateReadyTasks() {
        // Only the ones at the front of the queue are ready
        for (var vmId : pendingTaskQueueByVm.keySet()) {
            var queue = pendingTaskQueueByVm.get(vmId);
            if (!queue.isEmpty() && !readyTaskByVm.containsKey(vmId)) {
                var readyTaskId = queue.peek();
                if (pendingDependencies.hasNoDependency(readyTaskId)) {
                    queue.poll();
                    readyTaskByVm.put(vmId, readyTaskId);
                }
            }
        }
    }

    /// A private record to represent a VM ID.
    private record VmId(int vmId) {
    }

    /// A private record to represent a workflow task ID.
    private record WorkflowTaskId(int workflowId, int taskId) {
    }
}
