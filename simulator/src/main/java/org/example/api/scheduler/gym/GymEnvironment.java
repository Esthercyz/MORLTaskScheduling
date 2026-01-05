package org.example.api.scheduler.gym;

import org.example.api.scheduler.gym.types.AgentResult;

/// Represents an environment that interacts with a Gym agent.
/// This is called from the Java side. // Java端调用此类与Python端智能体交互
public class GymEnvironment<TObservation, TAction> {
    private final GymSharedQueue<TObservation, TAction> queue;

    public GymEnvironment(GymSharedQueue<TObservation, TAction> queue) {
        this.queue = queue;
    }

    /// Takes a step in the environment.
    // 把任意类型的观察和动作通过一个共享队列进行传递，把当前的环境观测交给队列，然后等待并返回agent给出的动作
    /// This will set the observation and block, waiting for the agent to return an action.
    public TAction step(AgentResult<TObservation> observation) {
        queue.setObservation(observation);
        return queue.getAction();
    }
}
