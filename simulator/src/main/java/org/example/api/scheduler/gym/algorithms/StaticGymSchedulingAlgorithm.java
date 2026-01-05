package org.example.api.scheduler.gym.algorithms;

import lombok.NonNull;
import org.example.api.dtos.TaskDto;
import org.example.api.dtos.VmAssignmentDto;
import org.example.api.dtos.VmDto;
import org.example.api.scheduler.gym.GymEnvironment;
import org.example.api.scheduler.gym.GymSharedQueue;
import org.example.api.scheduler.gym.types.AgentResult;
import org.example.api.scheduler.gym.types.StaticAction;
import org.example.api.scheduler.gym.types.StaticObservation;
import org.example.api.scheduler.internal.algorithms.StaticSchedulingAlgorithm;

import java.util.*;

/// A scheduling algorithm that delegates the scheduling to external gym environment.
public class StaticGymSchedulingAlgorithm implements StaticSchedulingAlgorithm {
    private final GymEnvironment<StaticObservation, StaticAction> environment;

// 从共享队列创建gymenvironment实例
    public StaticGymSchedulingAlgorithm(@NonNull GymSharedQueue<StaticObservation, StaticAction> queue) {
        this.environment = new GymEnvironment<>(queue);
    }

    @Override
    public List<VmAssignmentDto> schedule(@NonNull List<TaskDto> tasks, @NonNull List<VmDto> vms) {
        var observation = new StaticObservation(tasks, vms); //把当前的任务列表和vm列表封装成一个staticObservation，交给environment.step进一步决策
        var action = environment.step(AgentResult.reward(observation, 0));
        return action.getAssignments(); //把返回的action中的任务-虚拟机分配列表返回给调用者
    }
}
