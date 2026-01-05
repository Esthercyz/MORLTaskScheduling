package org.example.dataset;

import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class DatasetVmAssignment { //定义虚拟机和主机之间的分配关系
    private final int workflowId;
    private final int taskId;
    private final int vmId;
    private final double startTime;
    private final double endTime;
}
