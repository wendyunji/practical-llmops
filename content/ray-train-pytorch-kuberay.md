> **TL;DR**  
> 학습 함수는 프레임워크 코드에 집중하고, worker 수·GPU·체크포인트·스토리지는 Ray Train 설정으로 분리한다.

이 글은 Ray와 KubeRay를 실제 ML 플랫폼 관점에서 이해하기 위해 정리한 학습 기록입니다. 개념 소개에 그치지 않고, 어떤 운영 문제를 해결하는지와 확인해야 할 지점을 중심으로 구성했습니다.

**다루는 주제**: Ray Train · PyTorch · Distributed Training · Checkpoint

---

## Ray Train: 확장 가능한 모델 학습

> Ray Train은 분산 학습과 파인튜닝을 위한 확장 가능한 머신러닝 라이브러리입니다.
> Ray Train을 사용하면 단일 머신에서 작성한 학습 코드를 클라우드의 여러 머신 클러스터로 확장할 수 있으며, 분산 컴퓨팅의 복잡성을 추상화해 줍니다. 큰 모델이든, 큰 데이터셋이든 상관없이 Ray Train은 분산 학습을 가장 간단하게 구현할 수 있는 솔루션입니다.

Ray Train은 여러 프레임워크를 지원합니다:

**PyTorch 생태계**
- PyTorch
- PyTorch Lightning
- Hugging Face Transformers
- Hugging Face Accelerate
- DeepSpeed

**추가 프레임워크**
- TensorFlow
- Keras
- Horovod
- XGBoost
- LightGBM

## Ray Train 설치

---

Ray Train을 설치하려면 다음을 실행하세요:

```
pip install -U "ray[train]"
```

Ray와 그 라이브러리 설치에 대한 자세한 내용은 *Installing Ray* 문서를 참고하세요.

### run

```text
$ pip install -U "ray[train]"
Successfully installed fsspec-2025.10.0 numpy-2.0.2 pandas-2.3.3 pyarrow-21.0.0 python-dateutil-2.9.0.post0 pytz-2025.2 ray-2.51.2 tensorboardX-2.6.4 tzdata-2025.2
WARNING: You are using pip version 21.2.4; however, version 25.3 is available.
```

## 시작하기

---
- 개요: Ray Train을 이용한 분산 학습의 핵심 개념을 이해합니다.
- PyTorch: Ray Train과 PyTorch를 사용해 분산 모델 학습을 시작합니다.
- PyTorch Lightning: Ray Train과 Lightning을 사용해 분산 모델 학습을 시작합니다.
- Hugging Face Transformers: Ray Train과 Transformers로 분산 모델 학습을 시작합니다.
- JAX: Ray Train과 JAX를 사용해 분산 모델 학습을 시작합니다.

## 더 알아보기

---
- 추가 프레임워크: 원하는 프레임워크가 없나요? 관련 가이드를 확인하세요.
- 사용자 가이드: Ray Train으로 일반적인 모델 학습 작업을 수행하는 방법을 알아봅니다.
- 예제: 다양한 사용 사례에 대한 엔드투엔드 코드 예제를 살펴봅니다.
- API: Ray Train API에 대한 전체 설명은 API Reference 문서를 참고하세요.

## **Ray Train 개요**

> Training function(훈련 함수)
>
> Worker(작업자)
>
> Scaling configuration(스케일링 설정)
>
> Trainer(트레이너)

## **Training function**

---

훈련 함수는 사용자 정의 Python 함수이며, 모델 훈련 루프 전체의 로직을 담고 있다. 분산 훈련 작업을 실행할 때 각 worker는 이 훈련 함수를 실행한다.

Ray Train 문서에서는 다음과 같은 관례를 사용한다:
- **train_func** 는 훈련 코드를 포함한 사용자 정의 함수이다.
- **train_func** 는 Trainer의 **train_loop_per_worker** 파라미터로 전달된다.

```python
def train_func():
    """User-defined training function that runs on each distributed worker process.

    This function typically contains logic for loading the model,
    loading the dataset, training the model, saving checkpoints,
    and logging metrics.
    """
    ...
```

## **Worker**

---

Ray Train은 모델 훈련 계산을 클러스터 전체의 개별 worker 프로세스로 분산한다. 각 worker는 `train_func`을 실행하는 프로세스이다.

worker 수는 훈련 작업의 병렬성을 결정하며 `**[ScalingConfig](https://docs.ray.io/en/latest/train/api/doc/ray.train.ScalingConfig.html)**`**에서 설정된다**.

## **Scaling configuration**

---

`**[ScalingConfig](https://docs.ray.io/en/latest/train/api/doc/ray.train.ScalingConfig.html)**`는 훈련 작업의 스케일을 정의하는 메커니즘이다. 작업자 병렬성과 컴퓨트 리소스를 위해 두 가지 기본 파라미터를 지정한다:
- **num_workers** : 분산 훈련을 위해 실행할 worker 수
- **use_gpu** : 각 worker가 GPU를 사용할지 CPU를 사용할지 여부

```python
from ray.train import ScalingConfig

# Single worker with a CPU
scaling_config = ScalingConfig(num_workers=1, use_gpu=False)

# Single worker with a GPU
scaling_config = ScalingConfig(num_workers=1, use_gpu=True)

# Multiple workers, each with a GPU
scaling_config = ScalingConfig(num_workers=4, use_gpu=True)
```

## **Trainer(트레이너)**

---

Trainer는 앞서 언급한 세 가지 개념을 묶어 분산 훈련 작업을 시작한다.

Ray Train은 서로 다른 프레임워크를 위한 `**[Trainer 클래스](https://docs.ray.io/en/latest/train/api/api.html)**`를 제공한다.

`**[fit()](https://docs.ray.io/en/latest/train/api/doc/ray.train.trainer.BaseTrainer.fit.html)**` 메서드를 호출하면 훈련 작업을 실행한다. 이 과정은 다음을 포함한다:
- scaling_config에 정의된 대로 worker를 실행한다.
- 모든 worker에서 해당 프레임워크의 분산 환경을 설정한다.
- 모든 worker에서 train_func 를 실행한다.

```python
from ray.train.torch import TorchTrainer

trainer = TorchTrainer(train_func, scaling_config=scaling_config)
trainer.fit()
```

## **Get Started with Distributed Training using PyTorch**

> 이 튜토리얼은 기존 PyTorch 스크립트를 Ray Train을 사용하도록 변환하는 과정을 단계별로 설명한다.

다음 내용을 학습하게 된다:
- 모델을 분산 실행 및 올바른 CPU/GPU 디바이스에 배치하도록 설정
- 데이터로더를 구성하여 데이터를 워커별로 샤딩하고 적절한 CPU/GPU 디바이스에 배치
- 훈련 함수에서 메트릭을 보고하고 체크포인트를 저장하도록 구성
- 훈련 작업의 스케일링 및 CPU 또는 GPU 리소스 요구사항 구성
- TorchTrainer 클래스를 사용해 분산 훈련 작업 실행

## Quickstart

---

참고로, 최종 코드는 다음과 같은 형태가 될 것이다:

```python
from ray.train.torch import TorchTrainer
from ray.train import ScalingConfig

def train_func():
    # Your PyTorch training code here.
    ...

scaling_config = ScalingConfig(num_workers=2, use_gpu=True)
trainer = TorchTrainer(train_func, scaling_config=scaling_config)
result = trainer.fit()
```
- train_func 는 각 분산 훈련 워커에서 실행되는 Python 코드다.
- ScalingConfig 는 분산 훈련 워커 수와 GPU 사용 여부를 정의한다.
- TorchTrainer 는 분산 훈련 작업을 실행한다.

### 일반 코드와 비교하기

```powershell
import os
import tempfile

import torch
from torch.nn import CrossEntropyLoss
from torch.optim import Adam
from torch.utils.data import DataLoader
from torchvision.models import resnet18
from torchvision.datasets import FashionMNIST
from torchvision.transforms import ToTensor, Normalize, Compose

import ray.train.torch

def train_func():
    # Model, Loss, Optimizer
    model = resnet18(num_classes=10)
    model.conv1 = torch.nn.Conv2d(
        1, 64, kernel_size=(7, 7), stride=(2, 2), padding=(3, 3), bias=False
    )
    # [1] Prepare model.
    model = ray.train.torch.prepare_model(model)
    # model.to("cuda")  # This is done by `prepare_model`-> 원래 코드에는 이것만 있었음
    criterion = CrossEntropyLoss()
    optimizer = Adam(model.parameters(), lr=0.001)

    # Data
    transform = Compose([ToTensor(), Normalize((0.28604,), (0.32025,))])
    data_dir = os.path.join(tempfile.gettempdir(), "data")
    train_data = FashionMNIST(root=data_dir, train=True, download=True, transform=transform)
    train_loader = DataLoader(train_data, batch_size=128, shuffle=True)
    # [2] Prepare dataloader.
    train_loader = ray.train.torch.prepare_data_loader(train_loader)

    # Training
    for epoch in range(10):
        if ray.train.get_context().get_world_size() > 1:
            train_loader.sampler.set_epoch(epoch)

        for images, labels in train_loader:
            # This is done by `prepare_data_loader`!
            # images, labels = images.to("cuda"), labels.to("cuda")-> 원래 코드에는 이부분이 있었음
            outputs = model(images)
            loss = criterion(outputs, labels)
            optimizer.zero_grad()
            loss.backward()
            optimizer.step()

        # [3] Report metrics and checkpoint.
        metrics = {"loss": loss.item(), "epoch": epoch}
        with tempfile.TemporaryDirectory() as temp_checkpoint_dir:
            torch.save(
                model.module.state_dict(),
                os.path.join(temp_checkpoint_dir, "model.pt")
            )
            ray.train.report(
                metrics,
                checkpoint=ray.train.Checkpoint.from_directory(temp_checkpoint_dir),
            )
        if ray.train.get_context().get_world_rank() == 0:
            print(metrics)

# [4] Configure scaling and resource requirements.
scaling_config = ray.train.ScalingConfig(num_workers=2, use_gpu=True)

# [5] Launch distributed training job.
trainer = ray.train.torch.TorchTrainer(
    train_func,
    scaling_config=scaling_config,
    # [5a] If running in a multi-node cluster, this is where you
    # should configure the run's persistent storage that is accessible
    # across all worker nodes.
    # run_config=ray.train.RunConfig(storage_path="s3://..."),
)
result = trainer.fit()

# [6] Load the trained model.
with result.checkpoint.as_directory() as checkpoint_dir:
    model_state_dict = torch.load(os.path.join(checkpoint_dir, "model.pt"))
    model = resnet18(num_classes=10)
    model.conv1 = torch.nn.Conv2d(
        1, 64, kernel_size=(7, 7), stride=(2, 2), padding=(3, 3), bias=False
    )
    model.load_state_dict(model_state_dict)
```

## Set up a training function

---

먼저 훈련 코드를 분산 훈련을 지원하도록 업데이트한다. 코드 전체를 훈련 함수로 감싸는 것으로 시작한다:

```python
def train_func():
    # Your model training code here.
    ...
```

각 분산 훈련 워커는 이 함수를 실행한다.

또한 Trainer의 `train_loop_config`을 통해 딕셔너리 형태로 입력 인자를 지정할 수 있다. 예:

```python
def train_func(config):
    lr = config["lr"]
    num_epochs = config["num_epochs"]

config = {"lr": 1e-4, "num_epochs": 10}
trainer = ray.train.torch.TorchTrainer(train_func, train_loop_config=config, ...)
```
- Warning

큰 데이터 객체를 `train_loop_config`로 전달하는 것을 피하라.

직렬화/역직렬화 오버헤드가 커지기 때문이다. 대신, 대용량 객체는 `train_func` 내부에서 직접 초기화하는 것이 좋다.

```python
def load_dataset():
    # Return a large in-memory dataset
    ...

def load_model():
    # Return a large in-memory model instance
    ...

-config = {"data": load_dataset(), "model": load_model()}

def train_func(config):
-    data = config["data"]
-    model = config["model"]
+    data = load_dataset()
+    model = load_model()
     ...

 trainer = ray.train.torch.TorchTrainer(train_func, train_loop_config=config, ...)
```

## Set up a model

---

`ray.train.torch.prepare_model()` 유틸리티 함수 사용:
- 모델을 올바른 디바이스로 이동
- 모델을 DistributedDataParallel로 감싸기

변경 예:

```diff
-from torch.nn.parallel import DistributedDataParallel
+import ray.train.torch

 def train_func():

     ...

     model = ...
-    device_id = ... # Your logic to get the right device.
-    model = model.to(device_id or "cpu")
-    model = DistributedDataParallel(model, device_ids=[device_id])
+    model = ray.train.torch.prepare_model(model)

     ...
```

## Set up a dataset

---

`ray.train.torch.prepare_data_loader()` 사용 시:
- DataLoader 에 DistributedSampler 추가
- 배치를 올바른 디바이스로 자동 이동

Ray Data를 사용하는 경우에는 이 과정이 따로 필요하지 않다. → [https://docs.ray.io/en/latest/train/user-guides/data-loading-preprocessing.html#data-ingest-torch](https://docs.ray.io/en/latest/train/user-guides/data-loading-preprocessing.html)

예시:

```diff
 from torch.utils.data import DataLoader
+import ray.train.torch

 def train_func():

     ...

     dataset = ...

     data_loader = DataLoader(dataset, batch_size=worker_batch_size, shuffle=True)
+    data_loader = ray.train.torch.prepare_data_loader(data_loader)

     for epoch in range(10):
+        if ray.train.get_context().get_world_size() > 1:
+            data_loader.sampler.set_epoch(epoch)

         for X, y in data_loader:
-            X = X.to_device(device)
-            y = y.to_device(device)
```
- Tip

`DataLoader`의 `**batch_size**`**는 각 워커의 배치 크기(worker batch size)**다.

전체(global) 배치 크기는 다음과 같이 계산한다:

```
global_batch_size = worker_batch_size * ray.train.get_context().get_world_size()
```
- Note
  - 이미 DistributedSampler 를 수동으로 설정한 경우, prepare_data_loader() 는 새로 추가하지 않고 기존 sampler 설정을 존중한다.
  - IterableDataset 을 사용하는 DataLoader 는 DistributedSampler 가 동작하지 않는다. 이런 경우 Ray Data 사용을 권장한다.

## Report checkpoints and metrics

---

훈련 진행 상황을 모니터링하기 위해 ray.train.report() 를 사용하여 중간 메트릭과 체크포인트를 보고할 수 있다.

```diff
+import os
+import tempfile
+import ray.train

def train_func():

    ...

    with tempfile.TemporaryDirectory() as temp_checkpoint_dir:
        torch.save(
            model.state_dict(), os.path.join(temp_checkpoint_dir, "model.pt")
        )

+       metrics = {"loss": loss.item()}  # Training/validation metrics.

        # Build a Ray Train checkpoint from a directory
+       checkpoint = ray.train.Checkpoint.from_directory(temp_checkpoint_dir)

        # Ray Train will automatically save the checkpoint to persistent storage,
        # so the local `temp_checkpoint_dir` can be safely cleaned up after.
+       ray.train.report(metrics=metrics, checkpoint=checkpoint)
```

자세한 내용:
- Monitoring and Logging Metrics
- Saving and Loading Checkpoints

## Configure scale and GPUs

---

훈련 함수 외부에서 `ScalingConfig` 생성:
- num_workers : 분산 훈련 워커 수
- use_gpu : 각 워커가 GPU(or CPU) 사용할지 여부

```python
from ray.train import ScalingConfig
scaling_config = ScalingConfig(num_workers=2, use_gpu=True)
```
- 참고: https://docs.ray.io/en/latest/train/user-guides/using-gpus.html#train-scaling-config

## Configure persistent storage

---

RunConfig 로 체크포인트 및 결과 저장 경로 지정:

```python
from ray.train import RunConfig

# Local path
run_config = RunConfig(storage_path="/some/local/path", name="unique_run_name")

# Shared cloud storage
run_config = RunConfig(storage_path="s3://bucket", name="unique_run_name")

# Shared NFS path
run_config = RunConfig(storage_path="/mnt/nfs", name="unique_run_name")
```
- Warning 멀티 노드 클러스터에서 **공유 스토리지 설정은 필수**다. 로컬 경로만 지정하면 체크포인트 저장 시 에러가 발생한다.
  - 참고: https://docs.ray.io/en/latest/train/user-guides/persistent-storage.html#persistent-storage-guide

## Launch a training job

---

이제 `TorchTrainer`를 사용해 분산 훈련 작업을 실행할 수 있다.

```python
from ray.train.torch import TorchTrainer

trainer = TorchTrainer(
    train_func, scaling_config=scaling_config, run_config=run_config
)
result = trainer.fit()
```

## Access training results

---

훈련 완료 후 `Result` 객체가 반환된다. 이 객체에는 훈련 중 보고된 메트릭과 체크포인트 정보가 포함된다.

```
result.metrics     # 훈련 중 보고된 메트릭
result.checkpoint  # 마지막 체크포인트
result.path        # 로그 저장 경로
result.error       # 실패 시 예외 정보
```
- 참고: https://docs.ray.io/en/latest/train/user-guides/results.html#train-inspect-results

## KubeRay RayCluster에서 실행하기

---

### run

```powershell
$ ray job submit \
  --working-dir . \
  --runtime-env-json '{"pip": "requirements.txt"}' \
  -- python pytorch.py
~/.venv/ray-cli/lib/python3.9/site-packages/urllib3/__init__.py:35: NotOpenSSLWarning: urllib3 v2 only supports OpenSSL 1.1.1+, currently the 'ssl' module is compiled with 'LibreSSL 2.8.3'. See: https://github.com/urllib3/urllib3/issues/3020
  warnings.warn(
Job submission server address: https://ray.example.com/ray-api
2025-12-02 16:55:05,543 INFO dashboard_sdk.py:338 -- Uploading package gcs://_ray_pkg_74cb052dbeca542d.zip.
2025-12-02 16:55:05,544 INFO packaging.py:588 -- Creating a file package for local module '.'.

-------------------------------------------------------
Job 'raysubmit_uDGiTJEfuFcfRZsL' submitted successfully
-------------------------------------------------------

Next steps
  Query the logs of the job:
    ray job logs raysubmit_uDGiTJEfuFcfRZsL
  Query the status of the job:
    ray job status raysubmit_uDGiTJEfuFcfRZsL
  Request the job to be stopped:
# ... verbose output omitted ...
(RayTrainWorker pid=1522, ip=<node-ip>) {'loss': 0.034653112292289734, 'epoch': 9}

Training completed after 10 iterations at 2025-12-02 00:02:04. Total running time: 6min 53s
2025-12-02 00:02:04,421 INFO tune.py:1009 -- Wrote the latest version of all result files and experiment state to '/mnt/shared/ray_results/TorchTrainer_2025-12-01_23-55-10' in 0.0297s.

(RayTrainWorker pid=1259, ip=<node-ip>) Checkpoint successfully created at: Checkpoint(filesystem=local, path=/mnt/shared/ray_results/TorchTrainer_2025-12-01_23-55-10/TorchTrainer_39fb1_00000_0_2025-12-01_23-55-10/checkpoint_000009)

------------------------------------------
Job 'raysubmit_uDGiTJEfuFcfRZsL' succeeded
------------------------------------------
```
- [Ray] RayCluster → 기존 설정
- **Ray Cluster 추가 설정**

https://github.com/wendyunji/poc-kuberay/tree/main/ray-cluster
- Train Code

https://github.com/wendyunji/poc-kuberay/tree/main/tutorial/ray-train

---

## 운영 관점에서 남긴 결론

학습 함수는 프레임워크 코드에 집중하고, worker 수·GPU·체크포인트·스토리지는 Ray Train 설정으로 분리한다. 설정값을 외우기보다 제어면과 실행면, 워크로드 수명주기, 관측 가능한 성공 조건을 분리해 보는 것이 핵심입니다.

*실환경을 식별할 수 있는 호스트명·IP·내부 경로는 공개용 예시로 일반화했습니다.*
