from pathlib import Path

ROOT_PATH = Path(__file__).parent.parent.parent

# Data files
# ----------------------------------------------------------------------------------------------------------------------

DATA_PATH = ROOT_PATH / "data"
HOST_SPECS_PATH = DATA_PATH / "host_specs.json" //主机配置
WORKFLOW_FILES = [
    DATA_PATH / "pegasus_workflows" / "sipht.dag", //任务之间的父子关系
]


# Model related
# ----------------------------------------------------------------------------------------------------------------------

MAX_OBS_SIZE = 100_000 //可观测状态的最大尺寸
MAX_TRAINING_DS_SEED = 1_000_000_000 //训练数据集的最大随机种子
MIN_TESTING_DS_SEED = MAX_TRAINING_DS_SEED + 1 //测试数据集的最小随机种子
MIN_EVALUATING_DS_SEED = 2 * MAX_TRAINING_DS_SEED + 1 //评估数据集的最小随机种子

# Evaluation related
# ----------------------------------------------------------------------------------------------------------------------

DEFAULT_MODEL_DIR = ROOT_PATH / "logs"
ALGORITHMS = [
    ("Random", "random"),
    ("Round Robin", "round_robin"),
    ("Min-Min", "min_min"),
    ("Max-Min", "max_min"),
    ("HEFT", "insertion_heft"),
    ("Proposed", "gin:1733128027_gin_R=-diff:model_501760.pt"),
]
