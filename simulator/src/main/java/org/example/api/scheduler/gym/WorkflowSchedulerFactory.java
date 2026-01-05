package org.example.api.scheduler.gym;

import lombok.Setter;
import lombok.experimental.Accessors;
import org.example.api.scheduler.WorkflowScheduler;
import org.example.api.scheduler.gym.algorithms.StaticGymSchedulingAlgorithm;
import org.example.api.scheduler.gym.types.StaticAction;
import org.example.api.scheduler.gym.types.StaticObservation;
import org.example.api.scheduler.internal.BufferedStaticWorkflowScheduler;
import org.example.api.scheduler.internal.StaticWorkflowScheduler;
import org.example.api.scheduler.internal.algorithms.RoundRobinSchedulingAlgorithm;

/// Factory for creating WorkflowScheduler instances.
@Accessors(chain = true, fluent = true)
@Setter
public class WorkflowSchedulerFactory {
    private GymSharedQueue<StaticObservation, StaticAction> staticSharedQueue;

    /// Create a new WorkflowScheduler instance.
    public WorkflowScheduler create(String algorithm) {
        if (algorithm.equals("static:gym")) { //调度决策会委托到外部Gym智能体，通过共享队列进行交互
            return new StaticWorkflowScheduler(new StaticGymSchedulingAlgorithm(staticSharedQueue));
        } else if (algorithm.equals("static:round-robin")) {
            return new StaticWorkflowScheduler(new RoundRobinSchedulingAlgorithm());
        } else if (algorithm.startsWith("buffer:gym:")) {
            var bufferSize = Integer.parseInt(algorithm.split(":")[2]);
            var bufferTimeout = Integer.parseInt(algorithm.split(":")[3]);
            return new BufferedStaticWorkflowScheduler(bufferSize, bufferTimeout,
                    new StaticGymSchedulingAlgorithm(staticSharedQueue));
        }
        throw new IllegalArgumentException("Invalid algorithm: " + algorithm);
    }
}
