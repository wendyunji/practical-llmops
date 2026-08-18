> **TL;DR**  
> Ray Core가 분산 실행 기반을 제공하고 Data·Train·Tune·Serve가 ML 수명주기별 추상화를 더한다.

이 글은 Ray와 KubeRay를 실제 ML 플랫폼 관점에서 이해하기 위해 정리한 학습 기록입니다. 개념 소개에 그치지 않고, 어떤 운영 문제를 해결하는지와 확인해야 할 지점을 중심으로 구성했습니다.

**다루는 주제**: Ray · Ray Data · Ray Train · Ray Serve

---

## Getting Started

> Ray는 AI 및 Python 애플리케이션을 확장하기 위한 오픈 소스 통합 프레임워크다. 이는 노트북부터 클러스터까지 확장되는 분산 애플리케이션을 구축하기 위한 간단하고 보편적인 API를 제공한다.

## What’s Ray?

---

Ray는 다음을 제공하여 분산 컴퓨팅을 단순화한다:
- 확장 가능한 컴퓨트 기본 요소 : 병렬 프로그래밍을 쉽게 하기 위한 작업(tasks)과 액터(actors)
- 특화된 AI 라이브러리 : 데이터 처리, 모델 학습, 하이퍼파라미터 튜닝, 모델 서빙 등 일반적인 ML 워크로드를 위한 도구
- 통합 리소스 관리 : 자동 리소스 처리를 통해 노트북에서 클라우드까지의 매끄러운 확장성

## Choose Your Path

---

당신의 필요에 맞는 가이드를 선택하세요:
- ML 워크로드 확장: Ray Libraries Quickstart
- 일반적인 Python 애플리케이션 확장: Ray Core Quickstart
- 클라우드 배포: Ray Clusters Quickstart
- 애플리케이션 디버깅 및 모니터링: Debugging and Monitoring Quickstart

## Ray AI Libraries Quickstart

---

ML 워크로드를 위해 개별 라이브러리를 사용할 수 있다. 각 라이브러리는 데이터 처리부터 모델 서빙에 이르기까지 ML 워크플로의 특정 부분에 특화되어 있다. 아래에서 자신의 워크로드에 맞는 드롭다운을 클릭하면 된다.

### Ray Data

Ray Data는 머신러닝 및 AI 워크로드에 최적화된 분산 데이터 처리를 제공한다. 데이터 파이프라인을 통해 데이터를 효율적으로 스트리밍한다.

아래는 Ray Data를 사용해 오프라인 추론과 학습 입력(ingest)을 확장하는 방법의 예시다.

**Note**

이 예제를 실행하려면 Ray Data를 설치해야 한다:

```bash
pip install -U "ray[data]"
```

```python
from typing import Dict
import numpy as np
import ray

# Create datasets from on-disk files, Python objects, and cloud storage like S3.
ds = ray.data.read_csv("s3://anonymous@ray-example-data/iris.csv")

# Apply functions to transform data. Ray Data executes transformations in parallel.
def compute_area(batch: Dict[str, np.ndarray]) -> Dict[str, np.ndarray]:
    length = batch["petal length (cm)"]
    width = batch["petal width (cm)"]
    batch["petal area (cm^2)"] = length * width
    return batch

transformed_ds = ds.map_batches(compute_area)

# Iterate over batches of data.
for batch in transformed_ds.iter_batches(batch_size=4):
    print(batch)

# Save dataset contents to on-disk files or cloud storage.
transformed_ds.write_parquet("local:///tmp/iris/")
```

### Ray Train

Ray Train은 분산 모델 학습을 간단하게 만들어 준다. PyTorch와 TensorFlow 같은 인기 프레임워크 전반에 걸쳐 분산 학습을 설정하는 복잡성을 감춘다.

**Note**

이 예제를 실행하려면 Ray Train과 PyTorch 패키지를 설치해야 한다:

```bash
pip install -U "ray[train]" torch torchvision
```

먼저 데이터셋과 모델을 설정한다.

```python
import torch
import torch.nn as nn
from torch.utils.data import DataLoader
from torchvision import datasets
from torchvision.transforms import ToTensor

def get_dataset():
    return datasets.FashionMNIST(
        root="/tmp/data",
        train=True,
        download=True,
        transform=ToTensor(),
    )

class NeuralNetwork(nn.Module):
    def __init__(self):
        super().__init__()
        self.flatten = nn.Flatten()
        self.linear_relu_stack = nn.Sequential(
            nn.Linear(28 * 28, 512),
            nn.ReLU(),
            nn.Linear(512, 512),
            nn.ReLU(),
            nn.Linear(512, 10),
        )

    def forward(self, inputs):
        inputs = self.flatten(inputs)
        logits = self.linear_relu_stack(inputs)
        return logits
```

이제 단일 워커 PyTorch 학습 함수를 정의한다.

```python
def train_func():
    num_epochs = 3
    batch_size = 64

    dataset = get_dataset()
    dataloader = DataLoader(dataset, batch_size=batch_size)

    model = NeuralNetwork()

    criterion = nn.CrossEntropyLoss()
    optimizer = torch.optim.SGD(model.parameters(), lr=0.01)

    for epoch in range(num_epochs):
        for inputs, labels in dataloader:
            optimizer.zero_grad()
            pred = model(inputs)
            loss = criterion(pred, labels)
            loss.backward()
            optimizer.step()
        print(f"epoch: {epoch}, loss: {loss.item()}")
```

이 학습 함수는 다음과 같이 실행할 수 있다:

```python
train_func()
```

이제 이를 분산 다중 워커 학습 함수로 변환한다.

`ray.train.torch.prepare_model`과 `ray.train.torch.prepare_data_loader` 유틸리티 함수를 사용하여 모델과 데이터를 분산 학습용으로 설정한다. 이 함수들은 모델을 자동으로 `DistributedDataParallel`로 감싸고 올바른 디바이스에 올리며, `DataLoader`에 `DistributedSampler`를 추가해 준다.

```python
import ray.train.torch

def train_func_distributed():
    num_epochs = 3
    batch_size = 64

    dataset = get_dataset()
    dataloader = DataLoader(dataset, batch_size=batch_size, shuffle=True)
    dataloader = ray.train.torch.prepare_data_loader(dataloader)

    model = NeuralNetwork()
    model = ray.train.torch.prepare_model(model)

    criterion = nn.CrossEntropyLoss()
    optimizer = torch.optim.SGD(model.parameters(), lr=0.01)

    for epoch in range(num_epochs):
        if ray.train.get_context().get_world_size() > 1:
            dataloader.sampler.set_epoch(epoch)

        for inputs, labels in dataloader:
            optimizer.zero_grad()
            pred = model(inputs)
            loss = criterion(pred, labels)
            loss.backward()
            optimizer.step()
        print(f"epoch: {epoch}, loss: {loss.item()}")
```

4개의 워커를 가진 `TorchTrainer`를 생성하고, 이를 사용해 새 학습 함수를 실행한다.

```python
from ray.train.torch import TorchTrainer
from ray.train import ScalingConfig

# For GPU Training, set `use_gpu` to True.
use_gpu = False

trainer = TorchTrainer(
    train_func_distributed,
    scaling_config=ScalingConfig(num_workers=4, use_gpu=use_gpu)
)

results = trainer.fit()
```

GPU를 사용해 학습 작업을 가속하려면 GPU 환경을 구성한 뒤 `use_gpu`를 `True`로 설정하면 된다. 만약 GPU 환경이 없다면, Anyscale에서 자동 확장 GPU 클러스터와 통합된 개발 워크스페이스를 제공한다.

### Ray Tune

Ray Tune은 어떤 규모에서도 사용할 수 있는 하이퍼파라미터 튜닝 라이브러리다. 효율적인 분산 탐색 알고리즘을 사용해 모델의 최적 하이퍼파라미터를 자동으로 찾아준다. Tune을 사용하면 PyTorch, TensorFlow, Keras를 포함한 어떤 딥러닝 프레임워크든 10줄 미만의 코드로 멀티 노드 분산 하이퍼파라미터 스윕을 실행할 수 있다.

**Note**

이 예제를 실행하려면 Ray Tune을 설치해야 한다:

```bash
pip install -U "ray[tune]"
```

이 예시는 반복적 학습 함수를 사용해 작은 그리드 서치를 실행한다.

```python
from ray import tune

def objective(config):  # ①
    score = config["a"] ** 2 + config["b"]
    return {"score": score}

search_space = {  # ②
    "a": tune.grid_search([0.001, 0.01, 0.1, 1.0]),
    "b": tune.choice([1, 2, 3]),
}

tuner = tune.Tuner(objective, param_space=search_space)  # ③

results = tuner.fit()
print(results.get_best_result(metric="score", mode="min").config)
```

TensorBoard가 설치되어 있다면(`pip install tensorboard`), 모든 트라이얼 결과를 자동으로 시각화할 수 있다:

```bash
tensorboard --logdir ~/ray_results
```

### Ray Serve

Ray Serve는 ML 모델과 비즈니스 로직을 위한 확장 가능하고 프로그래머블한 서빙을 제공한다. 어떤 프레임워크의 모델이든 프로덕션급 성능으로 배포할 수 있다.

**Note**

이 예제를 실행하려면 Ray Serve와 scikit-learn을 설치해야 한다:

```bash
pip install -U "ray[serve]" scikit-learn
```

이 예시는 scikit-learn의 그래디언트 부스팅 분류기를 서빙한다.

```python
import requests
from starlette.requests import Request
from typing import Dict

from sklearn.datasets import load_iris
from sklearn.ensemble import GradientBoostingClassifier

from ray import serve

# Train model.
iris_dataset = load_iris()
model = GradientBoostingClassifier()
model.fit(iris_dataset["data"], iris_dataset["target"])

@serve.deployment
class BoostingModel:
    def __init__(self, model):
        self.model = model
        self.label_list = iris_dataset["target_names"].tolist()

    async def __call__(self, request: Request) -> Dict:
        payload = (await request.json())["vector"]
        print(f"Received http request with data {payload}")

        prediction = self.model.predict([payload])[0]
        human_name = self.label_list[prediction]
        return {"result": human_name}

# Deploy model.
serve.run(BoostingModel.bind(model), route_prefix="/iris")

# Query it!
sample_request_input = {"vector": [1.2, 1.0, 1.1, 0.9]}
response = requests.get(
    "http://localhost:8000/iris", json=sample_request_input)
print(response.text)
```

응답은 `{"result": "versicolor"}`를 보여준다.

### RLlib

RLlib는 다양한 강화학습(RL) 알고리즘의 고성능 구현체와 여러 학습 환경 지원을 제공하는 강화학습 라이브러리다. RLlib는 높은 확장성과, 다양한 산업 및 연구용 애플리케이션을 위한 통합 API를 제공한다.

**Note**

이 예제를 실행하려면 rllib과 tensorflow 또는 pytorch 중 하나를 설치해야 한다:

```bash
pip install -U "ray[rllib]" tensorflow  # or torch
```

시스템에 CMake를 설치해야 할 수도 있다.

```python
import gymnasium as gym
import numpy as np
import torch
from typing import Dict, Tuple, Any, Optional

from ray.rllib.algorithms.ppo import PPOConfig

# Define your problem using python and Farama-Foundation's gymnasium API:
class SimpleCorridor(gym.Env):
    """Corridor environment where an agent must learn to move right to reach the exit.

    ---------------------
    | S | 1 | 2 | 3 | G |   S=start; G=goal; corridor_length=5
    ---------------------

    Actions:
        0: Move left
        1: Move right

    Observations:
        A single float representing the agent's current position (index)
        starting at 0.0 and ending at corridor_length

    Rewards:
        -0.1 for each step
        +1.0 when reaching the goal

    Episode termination:
        When the agent reaches the goal (position >= corridor_length)
    """

    def __init__(self, config):
        self.end_pos = config["corridor_length"]
        self.cur_pos = 0.0
        self.action_space = gym.spaces.Discrete(2)  # 0=left, 1=right
        self.observation_space = gym.spaces.Box(0.0, self.end_pos, (1,), np.float32)

    def reset(
        self, *, seed: Optional[int] = None, options: Optional[Dict] = None
    ) -> Tuple[np.ndarray, Dict]:
        """Reset the environment for a new episode.

        Args:
            seed: Random seed for reproducibility
            options: Additional options (not used in this environment)

        Returns:
            Initial observation of the new episode and an info dict.
        """
        super().reset(seed=seed)  # Initialize RNG if seed is provided
        self.cur_pos = 0.0
        # Return initial observation.
        return np.array([self.cur_pos], np.float32), {}

    def step(self, action: int) -> Tuple[np.ndarray, float, bool, bool, Dict]:
        """Take a single step in the environment based on the provided action.

        Args:
            action: 0 for left, 1 for right

        Returns:
            A tuple of (observation, reward, terminated, truncated, info):
                observation: Agent's new position
                reward: Reward from taking the action (-0.1 or +1.0)
                terminated: Whether episode is done (reached goal)
                truncated: Whether episode was truncated (always False here)
                info: Additional information (empty dict)
        """
        # Walk left if action is 0 and we're not at the leftmost position
        if action == 0 and self.cur_pos > 0:
            self.cur_pos -= 1
        # Walk right if action is 1
        elif action == 1:
            self.cur_pos += 1
        # Set `terminated` flag when end of corridor (goal) reached.
        terminated = self.cur_pos >= self.end_pos
        truncated = False
        # +1 when goal reached, otherwise -0.1.
        reward = 1.0 if terminated else -0.1
        return np.array([self.cur_pos], np.float32), reward, terminated, truncated, {}
```

```python
# Create an RLlib Algorithm instance from a PPOConfig object.
print("Setting up the PPO configuration...")
config = (
    PPOConfig().environment(
        # Env class to use (our custom gymnasium environment).
        SimpleCorridor,
        # Config dict passed to our custom env's constructor.
        # Use corridor with 20 fields (including start and goal).
        env_config={"corridor_length": 20},
    )
    # Parallelize environment rollouts for faster training.
    .env_runners(num_env_runners=3)
    # Use a smaller network for this simple task
    .training(model={"fcnet_hiddens": [64, 64]})
)

# Construct the actual PPO algorithm object from the config.
algo = config.build_algo()
rl_module = algo.get_module()

# Train for n iterations and report results (mean episode rewards).
# Optimal reward calculation:
# - Need at least 19 steps to reach the goal (from position 0 to 19)
# - Each step (except last) gets -0.1 reward: 18 * (-0.1) = -1.8
# - Final step gets +1.0 reward
# - Total optimal reward: -1.8 + 1.0 = -0.8
print("\nStarting training loop...")
for i in range(5):
    results = algo.train()

    # Log the metrics from training results
    print(f"Iteration {i+1}")
    print(f"  Training metrics: {results['env_runners']}")

# Save the trained algorithm (optional)
checkpoint_dir = algo.save()
print(f"\nSaved model checkpoint to: {checkpoint_dir}")

print("\nRunning inference with the trained policy...")
# Create a test environment with a shorter corridor to verify the agent's behavior
env = SimpleCorridor({"corridor_length": 10})
# Get the initial observation (should be: [0.0] for the starting position).
obs, info = env.reset()
terminated = truncated = False
total_reward = 0.0
step_count = 0

# Play one episode and track the agent's trajectory
print("\nAgent trajectory:")
positions = [float(obs[0])]  # Track positions for visualization

while not terminated and not truncated and step_count < 1000:
    # Compute an action given the current observation
    action_logits = rl_module.forward_inference(
        {"obs": torch.from_numpy(obs).unsqueeze(0)}
    )["action_dist_inputs"].numpy()[
        0
    ]  # [0]: Batch dimension=1

    # Get the action with highest probability
    action = np.argmax(action_logits)

    # Log the agent's decision
    action_name = "LEFT" if action == 0 else "RIGHT"
    print(f"  Step {step_count}: Position {obs[0]:.1f}, Action: {action_name}")

    # Apply the computed action in the environment
    obs, reward, terminated, truncated, info = env.step(action)
    positions.append(float(obs[0]))

    # Sum up rewards
    total_reward += reward
    step_count += 1

# Report final results
print(f"\nEpisode complete:")
print(f"  Steps taken: {step_count}")
print(f"  Total reward: {total_reward:.2f}")
print(f"  Final position: {obs[0]:.1f}")

# Verify the agent has learned the optimal policy
if total_reward > -0.5 and obs[0] >= 9.0:
    print("  Success! The agent has learned the optimal policy (always move right).")
else:
    print("  Failure! The agent didn't reach the goal within 1000 timesteps.")
```

## Ray Core Quickstart

---

Ray Core는 분산 애플리케이션을 구축하고 실행하기 위한 간단한 기본 요소를 제공한다. 이를 사용하면 일반적인 Python 또는 Java 함수와 클래스를 몇 줄의 코드만으로 분산 무상태 태스크(stateless tasks)와 상태 유지 액터(stateful actors)로 변환할 수 있다.

아래 예시는 다음을 보여준다:
- Python 함수를 Ray 태스크로 변환해 병렬 실행하는 방법
- Python 클래스를 Ray 액터로 변환해 분산 상태 기반 계산을 수행하는 방법

### Parallelizing Functions with Ray Tasks

**Note**

이 예제를 실행하려면 Ray Core를 설치해야 한다:

```bash
pip install -U "ray"
```

Ray를 임포트하고 `ray.init()`으로 초기화한다. 그런 다음 함수에 `@ray.remote` 데코레이터를 붙여 이 함수를 원격으로 실행하고 싶다는 것을 선언한다. 마지막으로, 함수를 일반 호출 대신 `.remote()`로 호출한다. 이 원격 호출은 나중에 `ray.get`으로 가져올 수 있는 미래값, 즉 Ray 오브젝트 참조를 반환한다.

```python
import ray
ray.init()

@ray.remote
def f(x):
    return x * x

futures = [f.remote(i) for i in range(4)]
print(ray.get(futures)) # [0, 1, 4, 9]
```

### Parallelizing Classis with Ray Actors

**Note**

이 예제를 실행하려면 Ray Core를 설치해야 한다:

```bash
pip install -U "ray"
```

```python
import ray
ray.init() # Only call this once.

@ray.remote
class Counter(object):
    def __init__(self):
        self.n = 0

    def increment(self):
        self.n += 1

    def read(self):
        return self.n

counters = [Counter.remote() for i in range(4)]
[c.increment.remote() for c in counters]
futures = [c.read.remote() for c in counters]
print(ray.get(futures)) # [1, 1, 1, 1]
```

## Ray Cluster Quickstart

---

애플리케이션을 AWS, GCP, Azure 등 다양한 환경의 Ray 클러스터에 배포할 수 있으며, 기존 코드에 최소한의 변경만 필요하다.

[[Ray] RayCluster](https://app.notion.com/p/Ray-RayCluster-2b0af74bab62808582d4c7b721375649?pvs=21) → 이미 클러스터 구축 해둠

## Debugging and Monitoring Quickstart

---

내장된 관측 도구를 사용해 Ray 애플리케이션과 클러스터를 모니터링하고 디버깅할 수 있다. 이러한 도구는 애플리케이션의 성능을 이해하고 병목을 식별하는 데 도움을 준다.

### Ray Dashboard: Web GUI to monitor and debug Ray

Ray 대시보드는 실시간 시스템 메트릭, 노드 수준 리소스 모니터링, 작업 프로파일링, 태스크 시각화를 보여주는 시각적 인터페이스를 제공한다. 대시보드는 사용자가 Ray 애플리케이션의 성능을 이해하고 잠재적인 문제를 식별하는 데 도움이 되도록 설계되었다.

이미지:

`https://raw.githubusercontent.com/ray-project/Images/master/docs/new-dashboard/Dashboard-overview.png`

**Note**

대시보드를 시작하려면 다음과 같이 기본 설치를 수행한다:

```bash
pip install -U "ray[default]"
```

대시보드는 Ray 스크립트를 실행할 때 자동으로 사용 가능해진다. 기본 URL `http://localhost:8265`로 대시보드에 접근할 수 있다.

### Ray State APIs: CLI to access cluster states

Ray state API는 사용자가 CLI 또는 Python SDK를 통해 Ray의 현재 상태(스냅샷)에 쉽게 접근할 수 있도록 해준다.

**Note**

state API를 사용하기 시작하려면 다음과 같이 기본 설치를 수행한다:

```bash
pip install -U "ray[default]"
```

다음 코드를 실행한다.

```python
import ray
import time

ray.init(num_cpus=4)

@ray.remote
def task_running_300_seconds():
    print("Start!")
    time.sleep(300)

@ray.remote
class Actor:
    def __init__(self):
        print("Actor created")

# Create 2 tasks
tasks = [task_running_300_seconds.remote() for _ in range(2)]

# Create 2 actors
actors = [Actor.remote() for _ in range(2)]

ray.get(tasks)
```

터미널에서 `ray summary tasks`를 사용해 Ray 태스크의 요약 통계를 확인할 수 있다.

```bash
ray summary tasks
```

출력 예:

```
======== Tasks Summary: 2022-07-22 08:54:38.332537 ========
Stats:
------------------------------------
total_actor_scheduled: 2
total_actor_tasks: 0
total_tasks: 2

Table (group by func_name):
------------------------------------
FUNC_OR_CLASS_NAME        STATE_COUNTS    TYPE
0   task_running_300_seconds  RUNNING: 2      NORMAL_TASK
1   Actor.__init__            FINISHED: 2     ACTOR_CREATION_TASK
```

---

## 운영 관점에서 남긴 결론

Ray Core가 분산 실행 기반을 제공하고 Data·Train·Tune·Serve가 ML 수명주기별 추상화를 더한다. 설정값을 외우기보다 제어면과 실행면, 워크로드 수명주기, 관측 가능한 성공 조건을 분리해 보는 것이 핵심입니다.

*실환경을 식별할 수 있는 호스트명·IP·내부 경로는 공개용 예시로 일반화했습니다.*
