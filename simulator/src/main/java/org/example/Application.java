package org.example;

import lombok.Setter;
import org.cloudbus.cloudsim.Log;
import org.example.api.scheduler.gym.GymSharedQueue;
import org.example.api.scheduler.gym.WorkflowSchedulerFactory;
import org.example.api.scheduler.gym.types.AgentResult;
import org.example.api.scheduler.gym.types.StaticAction;
import org.example.api.scheduler.gym.types.StaticObservation;
import org.example.api.executor.LocalWorkflowExecutor;
import org.example.core.registries.CloudletRegistry;
import org.example.core.registries.HostRegistry;
import org.example.dataset.Dataset;
import org.example.sensors.RewardSensor;
import org.example.sensors.TaskStateSensor;
import org.example.simulation.SimulatedWorld;
import org.example.simulation.SimulatedWorldConfig;
import org.example.simulation.external.Py4JConnector;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

import java.io.File;
import java.util.concurrent.Callable;

@Setter
@Command(name = "cloudsim-simulator", mixinStandardHelpOptions = true, version = "1.0",
        description = "Runs a simulation of a workflow scheduling algorithm.",
        usageHelpAutoWidth = true)
public class Application implements Callable<Integer> {
    @Option(names = {"-f", "--file"}, description = "Dataset file")
    private File datasetFile;

    @Option(names = {"-p", "--port"}, description = "Py4J port", defaultValue = "25333")
    private int py4JPort;

    @Option(names = {"-a", "--algorithm"}, description = "Scheduling algorithm", defaultValue = "static:round-robin")
    private String algorithm;

    @Override
    public Integer call() throws Exception {
        System.err.println("Running simulation...");
        Log.disable();

        // Read input file or stdin 读取数据集（来自文件或标准输入），并根据数据集计算仿真总时长
        var dataset = datasetFile != null
                ? Dataset.fromFile(datasetFile)
                : Dataset.fromStdin();
        var duration = dataset.maximumHorizon();// 计算仿真总时长用于后续配置

        // Configure simulation
        var config = SimulatedWorldConfig.builder()
                .simulationDuration(duration)
                .monitoringUpdateInterval(1)
                .build();

        // Create shared queue
        var gymSharedQueue = new GymSharedQueue<StaticObservation, StaticAction>();

        // Create scheduler, and executor
        // 将共享队列注入到所选的静态或Gym调度算法中
        var scheduler = new WorkflowSchedulerFactory().staticSharedQueue(gymSharedQueue)
                .create(algorithm);
        var executor = new LocalWorkflowExecutor(); //构建本地任务执行器

        // Thread for Py4J connector
        var gymConnector = new Py4JConnector<>(py4JPort, gymSharedQueue);
        var gymThread = new Thread(gymConnector);
        gymThread.start(); //在独立线程中启动Py4J连接器
        // python能通过网关调用step/reset与Java环境交换观察/动作

        // Run simulation
        var world = SimulatedWorld.builder().dataset(dataset)
                .scheduler(scheduler).executor(executor)
                .config(config).build();
        var solution = world.runSimulation(); //启动并运行cloudsim

        // 仿真在时间队列耗尽或达到预设终止时结束，返回一个包含执行结果的对象
        var rewardSensor = RewardSensor.getInstance();
        var hostRegistry = HostRegistry.getInstance();
        var reward = rewardSensor.finalReward(duration);

        AgentResult<StaticObservation> finalAgentResult = AgentResult.truncated(reward);
        finalAgentResult.addInfo("solution", solution.toJson());
        finalAgentResult.addInfo("total_energy_consumption_j", Double.toString(hostRegistry.getTotalEnergyConsumptionJ()));
        finalAgentResult.addInfo("active_energy_consumption_j", Double.toString(hostRegistry.getActiveEnergyConsumptionJ()));
        gymSharedQueue.setObservation(finalAgentResult);  //将终结观测放回共享队列，供python端在收到截断/终止信号时读取并结束agent逻辑

        var cloudletRegistry = CloudletRegistry.getInstance();
        var taskStateSensor = TaskStateSensor.getInstance();
        cloudletRegistry.printSummaryTable();
        System.err.printf("Total makespan (s)           : %.5f%n", cloudletRegistry.getMakespan());
        System.err.printf("Total power consumption (W)  : %.2f%n", hostRegistry.getTotalPowerConsumptionW());
        System.err.printf("Total allocated VMs          : %d / %d%n", hostRegistry.getTotalAllocatedVms(), world.getBroker().getGuestList().size());
        System.err.printf("Unfinished Cloudlets         : %d / %d%n", cloudletRegistry.getRunningCloudletCount(), cloudletRegistry.getSize());
        System.err.printf("Total Cloudlet length (MI)   : %d%n", cloudletRegistry.getTotalCloudletLength());
        System.err.printf("Last task finish time (s)    : %.2f%n", cloudletRegistry.getLastCloudletFinishedAt());
        System.err.printf("Buffered Tasks               : %d%n", taskStateSensor.getBufferedTasks());
        System.err.printf("Scheduled Tasks              : %d%n", taskStateSensor.getScheduledTasks());
        System.err.printf("Executed Tasks               : %d%n", taskStateSensor.getExecutedTasks());
        System.err.printf("Finished Tasks               : %d%n", taskStateSensor.getCompletedTasks());
        System.out.println(solution.toJson());

        // Stop Py4J connector
        gymThread.join(5000);
        return 0;
    }

    public static void main(String[] args) {
        int exitCode = new CommandLine(new Application()).execute(args);
        System.exit(exitCode);
    }
}
