package org.example.api.scheduler.gym;

import org.example.api.scheduler.gym.types.AgentResult;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

/// Represents a shared queue between the Gym environment and the Gym agent.
/// This is used to communicate between the Java and Python sides.
public class GymSharedQueue<TObservation, TAction> {
    // 使用两个容量为1的阻塞队列实现Java和Python之间的同步通信
    // 传递环境到智能体的观察值
    private final BlockingQueue<AgentResult<TObservation>> observationQueue = new ArrayBlockingQueue<>(1);
    // 传递智能体到环境的动作值
    private final BlockingQueue<TAction> actionQueue = new ArrayBlockingQueue<>(1);

    /// Gets the observation from the queue.
    /// This will block until an observation is available.
    public AgentResult<TObservation> getObservation() {
        try {
            return observationQueue.take();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
    }

    /// Sets the observation in the queue.
    public void setObservation(AgentResult<TObservation> observation) {
        try {
            observationQueue.put(observation);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
    }

    /// Gets the action from the queue.
    /// This will block until an action is available.
    public TAction getAction() {
        try {
            return actionQueue.take();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
    }

    /// Sets the action in the queue.
    /// This will block until the action is available.
    public void setAction(TAction action) {
        try {
            actionQueue.put(action);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
    }
}
