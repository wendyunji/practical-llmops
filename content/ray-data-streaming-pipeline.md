> **TL;DR**  
> 전체 데이터를 한 번에 메모리에 올리지 않고 Block 단위로 읽고 변환하고 소비하면서 backpressure를 관리한다.

이 글은 Ray와 KubeRay를 실제 ML 플랫폼 관점에서 이해하기 위해 정리한 학습 기록입니다. 개념 소개에 그치지 않고, 어떤 운영 문제를 해결하는지와 확인해야 할 지점을 중심으로 구성했습니다.

**다루는 주제**: Ray Data · Streaming · Batch Inference · Ingest

---

## **Ray Data: ML을 위한 확장 가능한 데이터셋**

> ML·AI 워크로드 전용 확장형 데이터 처리 라이브러리
>
> 배치 추론(batch inference)
>
> 데이터 전처리
>
> 모델 학습용 데이터 ingest
>
> 전통적인 분산 데이터 시스템과 달리 Ray Data는 **스트리밍 기반 실행(Streaming execution)** 구조를 통해 대규모 데이터셋을 효율적으로 처리하고, CPU·GPU 자원을 높은 수준으로 활용한다.

## **왜 Ray Data인가?**

---

현대 AI 워크로드는 대부분 **딥러닝 모델**을 활용하며, 이는 GPU 같은 고가의 특수 연산 자원을 필요로 한다.

CPU와 달리 GPU는 **메모리가 작고**, **스케줄링 방식이 다르며**, **비용도 훨씬 비싸다**.

기존의 일반적인 데이터 처리 시스템들은 이런 특성을 고려하지 않아 GPU 활용률이 낮게 유지되는 문제가 많다.

Ray Data는 이러한 AI 워크로드를 first-class citizen으로 다루며, 다음과 같은 장점을 제공한다.

### **1. 딥러닝을 더 빠르고 저렴하게**

Ray Data는 **CPU 전처리 → GPU 추론/학습** 간 데이터를 스트리밍 방식으로 전달해

GPU가 놀지 않고 계속 작업하도록 하여 **GPU 활용률을 극대화**하고 **비용을 절감**한다.

### **2. AI 프레임워크 친화적**

Ray Data는 다음과 밀접히 통합되어 고성능으로 동작한다:
- vLLM
- PyTorch
- HuggingFace Transformers
- TensorFlow 또한 주요 클라우드(AWS, GCP, Azure)와도 잘 연동된다.

### **3. 멀티모달 데이터 지원**

Ray Data는 Apache Arrow·Pandas 기반이며 아래와 같은 ML 포맷을 기본 지원한다:
- Parquet, Lance
- 이미지(Image), JSON, CSV
- 오디오(Audio), 비디오(Video)
- 기타 다양한 ML 데이터 포맷

### **4. 기본적으로 확장 가능**

Ray 기반이므로 CPU·GPU가 혼합된 이기종 클러스터로 **자동 확장**되며,

코드는 변경 없이 **단일 머신 → 수백 노드 → 수백 TB 처리**로 스케일업된다.

## **설치**

---

Ray Data를 설치하려면 다음 명령을 실행한다.

```bash
pip install -U 'ray[data]'
```

Ray 및 라이브러리 설치에 대해 자세히 알아보려면 **Installing Ray** 문서를 참고하세요.

## **Ray Data 실제 사례 (Case Studies)**

---

### **Training ingest with Ray Data (학습 데이터 ingest)**
- Pinterest : 모델 학습을 위한 마지막 단계 데이터 처리에 Ray Data 활용
- DoorDash : 모델 학습 파이프라인을 Ray Data로 고도화
- Instacart : Ray Data 기반 분산 학습 파이프라인 구축
- Predibase : 이미지 증강(image augmentation) 처리 속도 대폭 향상

### **Batch inference with Ray Data (배치 추론)**
- ByteDance : 멀티모달 LLM 기반 오프라인 추론을 200 TB 규모로 확장
- Spotify : 신규 ML 플랫폼에서 배치 추론을 Ray Data로 구현
- Sewer AI : 대규모 비디오 객체 탐지 속도 3배 향상

## Ray Data Quickstart

> Ray Data의 Dataset 추상화를 사용해 분산 데이터 처리를 시작해보세요.

이 가이드는 Ray Data의 핵심 기능을 소개합니다:
- 데이터 로딩
- 데이터 변환
- 데이터 소비
- 데이터 저장

## Datasets

---

Ray Data의 주요 추상화는 **Dataset**이며, 이는 분산된 데이터 컬렉션을 나타냅니다. Dataset은 머신러닝 워크로드에 맞게 설계되었으며, 단일 머신 메모리를 초과하는 대규모 데이터도 효율적으로 처리할 수 있습니다.

## Loading data

---

Dataset은 로컬 파일, Python 객체, S3/GCS 같은 클라우드 스토리지 등 다양한 소스에서 생성할 수 있습니다. Ray Data는 Arrow가 지원하는 모든 파일시스템과 매끄럽게 연동됩니다.

```python
import ray

# S3에서 CSV 데이터셋 직접 로드
ds = ray.data.read_csv("s3://anonymous@air-example-data/iris.csv")

# 첫 번째 레코드 미리보기
ds.show(limit=1)
```

출력:

```
{'sepal length (cm)': 5.1, 'sepal width (cm)': 3.5,
 'petal length (cm)': 1.4, 'petal width (cm)': 0.2, 'target': 0}
```

데이터 생성에 대해 더 알아보려면 **Loading data** 문서를 참고하세요.

## Transforming data

---

사용자 정의 함수(UDF)를 적용하여 Dataset을 변환할 수 있습니다. Ray는 이러한 변환을 클러스터 전체에 자동으로 병렬화하여 성능을 향상시킵니다.

```python
from typing import Dict
import numpy as np

# "petal area" 속성을 계산하는 변환 정의
def transform_batch(batch: Dict[str, np.ndarray]) -> Dict[str, np.ndarray]:
    vec_a = batch["petal length (cm)"]
    vec_b = batch["petal width (cm)"]
    batch["petal area (cm^2)"] = vec_a * vec_b
    return batch

# Dataset에 변환 적용
transformed_ds = ds.map_batches(transform_batch)

# 새로운 컬럼이 포함된 스키마 보기
# .materialize()는 모든 lazy 변환을 실행하고
# Dataset을 오브젝트 스토어 메모리에 실체화함
print(transformed_ds.materialize())
```

출력 예시:

```
MaterializedDataset(
   num_blocks=...,
   num_rows=150,
   schema={
      sepal length (cm): double,
      sepal width (cm): double,
      petal length (cm): double,
      petal width (cm): double,
      target: int64,
      petal area (cm^2): double
   }
)
```

더 많은 변환 기능을 알아보려면 **Transforming data** 문서를 참고하세요.

## Consuming data

---

`take_batch()`나 `iter_batches()` 같은 편리한 메서드를 사용하여 Dataset의 내용을 접근할 수 있습니다. Dataset을 Ray Task나 Actor에 직접 넘겨 분산 처리에 사용할 수도 있습니다.

```python
# 처음 3개의 행을 배치로 추출하여 처리
print(transformed_ds.take_batch(batch_size=3))
```

출력:

```
{'sepal length (cm)': array([5.1, 4.9, 4.7]),
 'sepal width (cm)': array([3.5, 3. , 3.2]),
 'petal length (cm)': array([1.4, 1.4, 1.3]),
 'petal width (cm)': array([0.2, 0.2, 0.2]),
 'target': array([0, 0, 0]),
 'petal area (cm^2)': array([0.28, 0.28, 0.26])}
```

Dataset 내용 접근에 대해 더 알고 싶다면 **Iterating over Data**와 **Saving Data** 문서를 참고하세요.

## Saving data

---

처리된 Dataset을 `write_parquet()`, `write_csv()` 등 다양한 포맷과 저장소 위치로 내보낼 수 있습니다.

```python
import os

# 변환된 Dataset을 Parquet 파일로 저장
transformed_ds.write_parquet("/tmp/iris")

# 생성된 파일 확인
print(os.listdir("/tmp/iris"))
```

출력:

```
['..._000000.parquet', '..._000001.parquet']
```

### run

```powershell
$ ray job submit \
  --address "$RAY_ADDRESS" \
  --working-dir . \
  --runtime-env-json '{
    "pip": ["numpy", "pandas", "pyarrow", "ray[data]"]
  }' \
  -- python quickstart.py
~/.venv/ray-cli/lib/python3.9/site-packages/urllib3/__init__.py:35: NotOpenSSLWarning: urllib3 v2 only supports OpenSSL 1.1.1+, currently the 'ssl' module is compiled with 'LibreSSL 2.8.3'. See: https://github.com/urllib3/urllib3/issues/3020
  warnings.warn(
Job submission server address: https://ray.example.com/ray-api
2025-12-01 17:06:39,764 INFO dashboard_sdk.py:338 -- Uploading package gcs://_ray_pkg_2b08e8bc8ccdd011.zip.
2025-12-01 17:06:39,765 INFO packaging.py:588 -- Creating a file package for local module '.'.

-------------------------------------------------------
Job 'raysubmit_wiXYmZ4MHs1WhYCC' submitted successfully
-------------------------------------------------------

Next steps
  Query the logs of the job:
    ray job logs raysubmit_wiXYmZ4MHs1WhYCC
# ... verbose output omitted ...
- MapBatches(transform_batch)->Write: Tasks: 0; Queued blocks: 0; Resources: 0.0 CPU, 336.0B object store: 100%|██████████| 4.00/4.00 [00:02<00:00, 1.47 row/s]
- MapBatches(transform_batch)->Write: Tasks: 0; Queued blocks: 0; Resources: 0.0 CPU, 336.0B object store: 100%|██████████| 4.00/4.00 [00:02<00:00, 1.47 row/s]
2025-12-01 00:07:12,547 INFO dataset.py:4537 -- Data sink Parquet finished. 150 rows and 7.0KB data written.
Saved files: ['ray', 'iris', '10_000001_000000.parquet', '10_000002_000000.parquet', 'uv-a0ee1626d36ab781.lock']

Ray Data quickstart finished!

------------------------------------------
Job 'raysubmit_wiXYmZ4MHs1WhYCC' succeeded
------------------------------------------
```

### run_improved version

```powershell
$ ray job submit \
  --address "$RAY_ADDRESS" \
  --working-dir . \
  --runtime-env-json '{
    "pip": ["numpy", "pandas", "pyarrow", "ray[data]"]
  }' \
  -- python quickstart_improved.py
~/.venv/ray-cli/lib/python3.9/site-packages/urllib3/__init__.py:35: NotOpenSSLWarning: urllib3 v2 only supports OpenSSL 1.1.1+, currently the 'ssl' module is compiled with 'LibreSSL 2.8.3'. See: https://github.com/urllib3/urllib3/issues/3020
  warnings.warn(
Job submission server address: https://ray.example.com/ray-api
2025-12-01 17:55:02,485 INFO dashboard_sdk.py:338 -- Uploading package gcs://_ray_pkg_d765378f9f0f1aaf.zip.
2025-12-01 17:55:02,486 INFO packaging.py:588 -- Creating a file package for local module '.'.

-------------------------------------------------------
Job 'raysubmit_UZXLFLRyKA9BvEgd' submitted successfully
-------------------------------------------------------

Next steps
  Query the logs of the job:
    ray job logs raysubmit_UZXLFLRyKA9BvEgd
# ... verbose output omitted ...
                                                                                                                                  
- Write: Tasks: 0; Queued blocks: 0; Resources: 0.0 CPU, 168.0B object store: 100%|██████████| 4.00/4.00 [00:00<00:00, 61.8 row/s]
2025-12-01 00:55:30,293 INFO dataset.py:4537 -- Data sink Parquet finished. 150 rows and 7.0KB data written.
Saved files: ['ray', 'iris', '10_000001_000000.parquet', '10_000002_000000.parquet', '20_000001_000000.parquet', '20_000002_000000.parquet', '20_000003_000000.parquet', 'uv-a0ee1626d36ab781.lock']

Ray Data quickstart finished!

------------------------------------------
Job 'raysubmit_UZXLFLRyKA9BvEgd' succeeded
------------------------------------------
```

## Key Concepts

## Datasets and blocks

---

Ray Data에는 두 가지 주요 개념이 있다:
- Datasets
- Blocks

### Dataset

Dataset은 주요 사용자용 Python API다. 이는 분산 데이터 컬렉션을 나타내며, 데이터 로딩 및 처리 작업을 정의한다. 사용자는 일반적으로 다음과 같이 API를 사용한다:
- 외부 스토리지 또는 인메모리 데이터로부터 Dataset을 생성한다.
- 데이터에 변환을 적용한다.
- 출력 데이터를 외부 스토리지로 저장하거나, 학습 작업자에게 전달한다.

Dataset API는 **지연(lazy)** 방식이므로, show()와 같이 데이터셋을 구체화하거나 소비하기 전까지는 연산이 실제로 실행되지 않는다. 이를 통해 Ray Data는 실행 계획을 최적화하고 파이프라인 기반의 스트리밍 방식으로 연산을 수행할 수 있다.

### Block

Block은 데이터셋의 단일 파티션을 나타내는 행(row)들의 집합이다. Arrow 같은 컬럼 기반 포맷으로 표현되는 블록들은 Ray Data에서 처리의 기본 단위가 된다.
- 모든 데이터셋은 여러 블록으로 분할되며,
- 전체 데이터셋 처리는 블록 단위로 분산되고 병렬 처리된다 (블록은 대부분 서로 독립적이다).

Block은 Ray Data 데이터셋을 구성하는 기본 단위이며 Ray의 오브젝트 스토어에 저장된다. 데이터 처리는 블록 단위에서 병렬화된다.

아래 그림은 각각 1000개의 행을 가진 세 개의 블록으로 구성된 데이터셋을 시각화한 것이다. Ray Data는 데이터셋을 실행을 트리거한 프로세스(일반적으로 프로그램의 엔트리포인트인 driver)에 보유하며, 블록들은 Ray의 공유 메모리 오브젝트 스토어에 객체로 저장한다. 내부적으로 Ray Data는 Pandas DataFrame 또는 PyArrow Table 형태의 블록을 네이티브하게 처리할 수 있다.

## **Operators and Plans**

---

Ray Data는 효율적 실행을 위해 **두 단계의(planning) 계획 과정**을 사용한다. Dataset API로 프로그램을 작성하면 Ray Data는 먼저 **논리적 계획(logical plan)** 을 생성하는데, 이는 무엇을 수행해야 하는지에 대한 고수준 설명이다. 실행이 시작되면 이를 실제 실행 방법을 명시하는 **물리적 계획(physical plan)** 으로 변환한다.

아래 다이어그램은 전체 계획 과정의 흐름을 보여준다:

계획을 구성하는 기본 요소는 **operators(연산자)** 이다.
- 논리적 계획은 어떤 작업을 수행할지 기술하는 논리 연산자(Logical Operator) 로 구성된다. 예: `ReadOp`는 어떤 데이터를 읽을지 지정한다.
- 물리적 계획은 작업을 어떻게 실행할지 기술하는 물리 연산자(Physical Operator) 로 구성된다. 예: `TaskPoolMapOperator`는 데이터를 실제로 읽기 위해 Ray task를 실행한다.

다음은 Ray Data가 논리적 계획을 어떻게 구성하는지를 보여주는 간단한 예시다. 아래처럼 연산을 체이닝할 때마다 Ray Data는 내부적으로 논리적 계획을 생성한다:

```python
dataset = ray.data.range(100)
dataset = dataset.add_column("test", lambda x: x["id"] + 1)
dataset = dataset.select_columns("test")
```

이 데이터셋을 출력하면 다음과 같은 논리적 계획을 볼 수 있다:

```
Project
+- MapBatches(add_column)
   +- Dataset(schema={...})
```

실행이 시작되면 Ray Data는 논리적 계획을 최적화한 후, 실제 데이터 변환을 구현하는 일련의 물리 연산자로 변환한다. 이 변환 과정에서:
- 하나의 논리 연산자는 여러 물리 연산자로 분할될 수 있다. 예: `ReadOp` → `InputDataBuffer` + `TaskPoolMapOperator`
- 논리/물리 계획 모두 최적화 규칙을 통과한다. 예: `OperatorFusionRule` 은 여러 map 연산자를 결합하여 직렬화 오버헤드를 줄인다.

물리 연산자는 다음 방식으로 동작한다:
- 블록 참조(block references)의 스트림을 입력받고,
- 연산을 수행한다 (Ray Tasks/Actors로 데이터를 변환하거나 참조 자체를 조작),
- 새로운 블록 참조 스트림을 출력한다.

Ray Tasks 및 Actors에 대한 자세한 내용은 Ray Core Concepts 참고.

**Note**

데이터셋의 실행 계획은 show() 등의 연산을 통해 데이터셋이 materialize(구체화)되거나 소비될 때만 실행된다.

### 왜 두단계로 나눌까?

논리적 계획은 해야할 일의 순서만 기록해두고 실제로 실행은 하지 않음, 반면 물리적 계획은 어떻게 실행할 것인지에 대한 실제 스케쥴링/태스크 계획을 포함함. 따라서 물리적 계획 단계에서 어떤 워커가 실행될 것인지, 몇개의 task로 쪼개질 것인지, 블럭 단위로 어떻게 스트리밍할 것인지, 어떤 연산을 fusion(map 합치기)할 것인지 전략을 짜게 됨. 즉, 논리 계획의 한줄이 여러 줄의 물리 계획이 되는 것.SQL 엔진이 `SELECT ...`를 받으면 “논리적 계획 → 물리적 계획” 두 단계 거치는 것처럼Ray Data도 아래 이유 때문에 이렇게 함:실행 전에 최적화 가능 (map 연산자 여러 개를 fuse)자원(CPU, GPU) 최적 배치 가능block 단위 병렬화 최적화 가능스트리밍 파이프라인 구성 가능즉, Ray Data는 네가 편하게 `.map().filter()` 썼지만내부에서는 “대규모 분산 시스템처럼” 계획을 만들어서 실행하는 거.

## **Streaming execution model**

---

Ray Data는 대규모 데이터셋을 효율적으로 처리하기 위해 **스트리밍 실행 모델(streaming execution model)** 을 사용한다.

전체 데이터셋을 한 번에 메모리에 적재하는 대신, Ray Data는 연산 파이프라인을 통해 데이터를 스트리밍 방식으로 처리할 수 있다.

이는 전체 데이터가 메모리에 적재될 필요가 없고, 데이터셋이 너무 커서 메모리에 맞지 않는 경우 유용하다. 특히 추론(inference) 및 학습(training) 워크로드에 적합하다.

다음 예시는 스트리밍 모델이 어떻게 동작하는지를 보여준다:

```python
import ray

# 1K rows로 구성된 데이터셋 생성
ds = ray.data.read_csv("s3://anonymous@air-example-data/iris.csv")

# 연산 파이프라인 정의
ds = ds.map(lambda x: {"target1": x["target"] * 2})
ds = ds.map(lambda x: {"target2": x["target1"] * 2})
ds = ds.map(lambda x: {"target3": x["target2"] * 2})
ds = ds.filter(lambda x: x["target3"] % 4 == 0)

# show() 같은 메서드를 호출하면 데이터가 실제로 흐르기 시작함
ds.show(5)
```

이 코드는 다음과 같은 논리적 계획을 생성한다:

```
Filter(<lambda>)
+- Map(<lambda>)
   +- Map(<lambda>)
      +- Map(<lambda>)
         +- Dataset(schema={...})
```

스트리밍 토폴로지(연산 파이프라인 구조)는 다음과 같다:

스트리밍 실행 모델에서 연산자들은 파이프라인으로 연결되며, 각 연산자의 출력 큐는 다음 연산자의 입력 큐로 직접 전달된다. 이는 실행 계획 전체에서 매우 효율적인 데이터 흐름을 만든다.

이 스트리밍 모델은 여러 가지 장점을 제공한다:
- 파이프라인의 여러 단계가 동시에(concurrently) 실행될 수 있어 전체 성능과 자원 사용률을 향상한다.
- 예를 들어 map 연산자가 GPU 자원을 필요로 하고 filter 연산자가 CPU에서 실행될 수 있다면, 스트리밍 모델은 두 연산을 동시에 실행하여 GPU를 전체 파이프라인 동안 효율적으로 사용할 수 있다.

결론적으로, Ray Data의 스트리밍 실행 모델은 **가용 메모리보다 훨씬 큰 데이터셋을 처리하면서도, 클러스터 전체에서 병렬 실행을 통해 높은 성능을 유지**할 수 있다.

**Note**

`ds.sort()` 및 `ds.groupby()` 같은 연산은 데이터를 materialize(구체화)해야 하므로 아주 큰 데이터셋에서는 메모리 사용량에 영향을 미칠 수 있다.

### 이해를 위한 설명

**✔️ 예시로 감 잡기**

아래 코드가 있다고 해보자:

```python
ds = ray.data.read_csv("iris.csv")
ds = ds.map(fn1).map(fn2).filter(fn3)
ds.show()
```

일반적인 Pandas 사고방식이면:
1. CSV 로드 (전체 메모리로 가져옴)
1. map1 적용
1. map2 적용
1. filter 적용 → 끝날 때까지 기다림

**Ray는 이렇게 안 한다.**

Ray는 block 단위로 다음처럼 흐르게 함:

```
[Block1] → fn1 → fn2 → filter → output
[Block2] → fn1 → fn2 → filter → output
[Block3] → fn1 → fn2 → filter → output
...

시간 →
Block1: [Read] → [Map1] → [Map2] → [Filter] → [Output]

(조금 뒤)
Block2:       [Read] → [Map1] → [Map2] → [Filter] → [Output]

(조금 뒤)
Block3:                 [Read] → [Map1] → [Map2] → [Filter] → [Output]
```

즉 파이프라인이 마치 CPU 파이프라인처럼 **동시에 작동**함.

**✔️ 이것의 장점**

**① 전체 메모리에 다 안 넣어도 된다**

데이터가 100GB인데 메모리는 32GB여도 OK.

Block 200MB씩 쪼개서 흘려보내면 됨.

**② map/filter 연산들이 동시에 돌아간다**

Block1이 map1에서 처리되는 동안

Block0은 filter까지 이미 갔고

Block2는 읽는 중임.

즉 병렬 + 파이프라인 둘 다 사용.

**③ GPU/CPU를 모두 사용할 때 효율 최고**

예:
- map1은 GPU 연산
- filter는 CPU 연산

→ Streaming 모델에서는 GPU 작업이 끝날 때까지 기다릴 필요 없음

→ CPU는 다음 block을 계속 처리

---

## 운영 관점에서 남긴 결론

전체 데이터를 한 번에 메모리에 올리지 않고 Block 단위로 읽고 변환하고 소비하면서 backpressure를 관리한다. 설정값을 외우기보다 제어면과 실행면, 워크로드 수명주기, 관측 가능한 성공 조건을 분리해 보는 것이 핵심입니다.

*실환경을 식별할 수 있는 호스트명·IP·내부 경로는 공개용 예시로 일반화했습니다.*
