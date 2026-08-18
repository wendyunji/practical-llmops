> **TL;DR**  
> 상태가 없는 병렬 함수는 Task, 상태를 가진 장기 실행 워커는 Actor, 큰 데이터 전달은 ObjectRef로 모델링한다.

이 글은 Ray와 KubeRay를 실제 ML 플랫폼 관점에서 이해하기 위해 정리한 학습 기록입니다. 개념 소개에 그치지 않고, 어떤 운영 문제를 해결하는지와 확인해야 할 지점을 중심으로 구성했습니다.

**다루는 주제**: Ray Core · Tasks · Actors · Object Store

---

## **What’s Ray Core?**

> Ray Core는
>
> 필수 기본 구성 요소(Tasks, Actors, Objects)

Ray는 여러 GPU를 사용하는 애플리케이션에 특히 적합한 고성능 워크로드용 **실험적 API, Ray Compiled Graph** 을 도입했다. 자세한 내용은 Ray Compiled Graph 문서를 참조하라.

## **Getting Started**

---

시작하려면 다음 명령으로 Ray를 설치한다:

```
pip install -U ray
```

추가 설치 옵션은 **Installing Ray** 문서를 참고한다.

Ray를 사용하기 위한 첫 번째 단계는 다음과 같이 import 및 초기화하는 것이다:

```python
import ray

ray.init()
```

**Note**

명시적으로 `ray.init()`을 호출하지 않더라도,

Ray 원격 API를 처음 사용하면 자동으로 인자 없이 `ray.init()`가 호출된다.

### import ray in kuberay

```powershell
import ray

ray.init(address="auto")
```

## **Running a Task**

---

Tasks는 Ray 클러스터 전반에서 Python 함수를 병렬화하는 가장 간단한 방법이다.

Task를 생성하려면:
1. 함수에 @ray.remote 데코레이터를 붙여 원격 실행 대상임을 표시한다.
1. 함수를 일반 호출 대신 .remote() 로 호출한다.
1. 반환된 future(Ray object reference)를 ray.get() 으로 값을 가져온다.

간단한 예시는 다음과 같다:

```python
# Define the square task.
@ray.remote
def square(x):
    return x * x

# Launch four parallel square tasks.
futures = [square.remote(i) for i in range(4)]

# Retrieve results.
print(ray.get(futures))
# -> [0, 1, 4, 9]
```

### run in ray cluster with ray cli

```powershell
$ source ~/.venv/ray-cli/bin/activate

$ ray job submit \                   
  --address $RAY_ADDRESS \
  --working-dir . \
  --runtime-env-json '{"pip": ["numpy"]}' \
  -- python task_example.py
~/.venv/ray-cli/lib/python3.9/site-packages/urllib3/__init__.py:35: NotOpenSSLWarning: urllib3 v2 only supports OpenSSL 1.1.1+, currently the 'ssl' module is compiled with 'LibreSSL 2.8.3'. See: https://github.com/urllib3/urllib3/issues/3020
  warnings.warn(
Job submission server address: https://ray.example.com/ray-api
2025-12-01 11:41:45,723 INFO dashboard_sdk.py:338 -- Uploading package gcs://_ray_pkg_79b789ca6054128d.zip.
2025-12-01 11:41:45,723 INFO packaging.py:588 -- Creating a file package for local module '.'.

-------------------------------------------------------
Job 'raysubmit_sYpSme2Cp8DmsCRf' submitted successfully
-------------------------------------------------------

Next steps
  Query the logs of the job:
    ray job logs raysubmit_sYpSme2Cp8DmsCRf
  Query the status of the job:
    ray job status raysubmit_sYpSme2Cp8DmsCRf
  Request the job to be stopped:
    ray job stop raysubmit_sYpSme2Cp8DmsCRf

Tailing logs until the job exits (disable with --no-wait):
2025-11-30 18:41:45,831 INFO job_manager.py:531 -- Runtime env is setting up.
2025-11-30 18:41:51,088 INFO worker.py:1554 -- Using address <node-ip>:6379 set in the environment variable RAY_ADDRESS
2025-11-30 18:41:51,089 INFO worker.py:1694 -- Connecting to existing Ray cluster at address: <node-ip>:6379...
2025-11-30 18:41:51,101 INFO worker.py:1879 -- Connected to Ray cluster. View the dashboard at <node-ip>:8265 
[0, 1, 4, 9]

------------------------------------------
Job 'raysubmit_sYpSme2Cp8DmsCRf' succeeded
------------------------------------------
```

## **Calling an Actor**

---

Tasks가 **stateless(무상태)** 인 반면,

Ray actors는 **상태(state)** 를 메서드 호출 사이에 유지하는 **stateful worker**를 만들 수 있게 한다.

Actor를 인스턴스화하면:
- Ray는 클러스터 어디엔가 actor 전용 워커 프로세스를 시작한다.
- Actor의 메서드는 해당 전용 worker에서 실행되며, 내부 상태에 접근·수정할 수 있다.
- Actor는 메서드 호출을 받은 순서대로 직렬(serially) 실행하여 일관성을 보장한다.

다음은 간단한 Counter 예제이다:

```python
# Define the Counter actor.
@ray.remote
class Counter:
    def __init__(self):
        self.i = 0

    def get(self):
        return self.i

    def incr(self, value):
        self.i += value

# Create a Counter actor.
c = Counter.remote()

# Submit calls to the actor. These calls run asynchronously but in
# submission order on the remote actor process.
for _ in range(10):
    c.incr.remote(1)

# Retrieve final actor state.
print(ray.get(c.get.remote()))
# -> 10
```

이전 예시는 기본적인 actor 사용법을 보여준다.

Tasks와 actors를 함께 사용하는 좀 더 포괄적인 예시는 **Monte Carlo Pi estimation example**을 참고하라.

### run in ray cluster with ray cli

```powershell
$ ray job submit \
  --address $RAY_ADDRESS \
  --working-dir . \
  -- python actor_example.py

~/.venv/ray-cli/lib/python3.9/site-packages/urllib3/__init__.py:35: NotOpenSSLWarning: urllib3 v2 only supports OpenSSL 1.1.1+, currently the 'ssl' module is compiled with 'LibreSSL 2.8.3'. See: https://github.com/urllib3/urllib3/issues/3020
  warnings.warn(
Job submission server address: https://ray.example.com/ray-api
2025-12-01 11:50:26,133 INFO dashboard_sdk.py:338 -- Uploading package gcs://_ray_pkg_aef5c684338914ec.zip.
2025-12-01 11:50:26,134 INFO packaging.py:588 -- Creating a file package for local module '.'.

-------------------------------------------------------
Job 'raysubmit_TRqDZ87YNJLARU44' submitted successfully
-------------------------------------------------------

Next steps
  Query the logs of the job:
    ray job logs raysubmit_TRqDZ87YNJLARU44
  Query the status of the job:
    ray job status raysubmit_TRqDZ87YNJLARU44
  Request the job to be stopped:
    ray job stop raysubmit_TRqDZ87YNJLARU44

Tailing logs until the job exits (disable with --no-wait):
2025-11-30 18:50:26,235 INFO job_manager.py:531 -- Runtime env is setting up.
2025-11-30 18:50:27,614 INFO worker.py:1554 -- Using address <node-ip>:6379 set in the environment variable RAY_ADDRESS
2025-11-30 18:50:27,614 INFO worker.py:1694 -- Connecting to existing Ray cluster at address: <node-ip>:6379...
2025-11-30 18:50:27,625 INFO worker.py:1879 -- Connected to Ray cluster. View the dashboard at <node-ip>:8265 
10

------------------------------------------
Job 'raysubmit_TRqDZ87YNJLARU44' succeeded
------------------------------------------
```

## **Passing Objects**

---

Ray의 분산 객체 저장소(distributed object store)는 클러스터 전반의 데이터를 효율적으로 관리한다. Ray에서는 객체를 다음 세 가지 방식으로 다룬다:

### **1. 암묵적 생성(Implicit creation)**

Tasks와 actors가 값을 반환하면, 그 값은 자동으로 Ray 객체 저장소에 저장되고,

나중에 가져올 수 있는 객체 참조(object reference)가 반환된다.

### **2. 명시적 생성(Explicit creation)**

`ray.put()` 을 사용해 직접 객체를 object store에 넣을 수 있다.

### **3. 참조 전달(Passing references)**

객체 자체가 아니라 **object reference를 다른 task/actor에 전달**할 수 있다.

이는 불필요한 데이터 복사를 방지하고, lazy execution을 가능하게 한다.

예시는 다음과 같다:

```python
import numpy as np

# Define a task that sums the values in a matrix.
@ray.remote
def sum_matrix(matrix):
    return np.sum(matrix)

# Call the task with a literal argument value.
print(ray.get(sum_matrix.remote(np.ones((100, 100)))))
# -> 10000.0

# Put a large array into the object store.
matrix_ref = ray.put(np.ones((1000, 1000)))

# Call the task with the object reference as an argument.
print(ray.get(sum_matrix.remote(matrix_ref)))
# -> 1000000.0
```

### run in ray cluster with ray cli

```powershell
$ ray job submit \
  --address $RAY_ADDRESS \
  --working-dir . \
  --runtime-env-json '{"pip": ["numpy"]}' \
  -- python object_example.py

~/.venv/ray-cli/lib/python3.9/site-packages/urllib3/__init__.py:35: NotOpenSSLWarning: urllib3 v2 only supports OpenSSL 1.1.1+, currently the 'ssl' module is compiled with 'LibreSSL 2.8.3'. See: https://github.com/urllib3/urllib3/issues/3020
  warnings.warn(
Job submission server address: https://ray.example.com/ray-api
2025-12-01 13:09:28,189 INFO dashboard_sdk.py:338 -- Uploading package gcs://_ray_pkg_f240a2c5823891e5.zip.
2025-12-01 13:09:28,190 INFO packaging.py:588 -- Creating a file package for local module '.'.

-------------------------------------------------------
Job 'raysubmit_hNDyZUQEYG9DGVUt' submitted successfully
-------------------------------------------------------

Next steps
  Query the logs of the job:
    ray job logs raysubmit_hNDyZUQEYG9DGVUt
  Query the status of the job:
    ray job status raysubmit_hNDyZUQEYG9DGVUt
  Request the job to be stopped:
    ray job stop raysubmit_hNDyZUQEYG9DGVUt

Tailing logs until the job exits (disable with --no-wait):
2025-11-30 20:09:28,281 INFO job_manager.py:531 -- Runtime env is setting up.
2025-11-30 20:09:29,798 INFO worker.py:1554 -- Using address <node-ip>:6379 set in the environment variable RAY_ADDRESS
2025-11-30 20:09:29,798 INFO worker.py:1694 -- Connecting to existing Ray cluster at address: <node-ip>:6379...
2025-11-30 20:09:29,810 INFO worker.py:1879 -- Connected to Ray cluster. View the dashboard at <node-ip>:8265 
10000.0
1000000.0

------------------------------------------
Job 'raysubmit_hNDyZUQEYG9DGVUt' succeeded
------------------------------------------
```

## **Key Concepts**

> 이 섹션에서는 Ray의 핵심 개념들을 개괄한다.
> 이러한 기본 구성 요소(primitives)는 서로 결합되어 Ray가 매우 다양한 분산 애플리케이션을 유연하게 지원하도록 한다.

## **Tasks**

---

Ray는 임의의 함수를 별도의 워커 프로세스에서 비동기적으로 실행할 수 있게 한다.

이러한 비동기 Ray 함수들을 **tasks(태스크)** 라고 부른다.

Ray는 태스크가 필요한 리소스(CPU, GPU, 커스텀 리소스)를 명시할 수 있도록 지원한다.

클러스터 스케줄러는 이러한 리소스 요청을 기반으로 태스크를 클러스터 전체에 분산시켜 병렬 실행을 가능하게 한다.

## **Actors**

---

Actors는 Ray API를 “함수(태스크)”에서 “클래스”까지 확장한다.

Actor는 본질적으로 **상태를 가진(stateful) 워커(또는 서비스)** 이다.

새 actor를 인스턴스화하면 Ray는 새로운 워커를 생성하고, actor의 메서드를 해당 워커에 스케줄링한다.

메서드는 해당 워커의 내부 상태에 접근하거나 상태를 변경할 수 있다.

태스크와 마찬가지로, actors 역시 CPU, GPU, 커스텀 리소스 요구 사항을 지정할 수 있다.

## **Objects**

---

태스크와 actor는 객체를 생성하고 객체에 대해 계산을 수행한다.

이 객체들을 **remote objects(원격 객체)** 라고 부르는데, 이는 Ray가 클러스터 어디든 객체를 저장할 수 있고, 사용자는 객체 참조(object ref)를 통해 이를 가리킬 수 있기 때문이다.

Ray는 원격 객체를 분산된 공유 메모리 기반 object store에 캐싱하며, 클러스터의 각 노드마다 하나의 object store가 존재한다.

클러스터 환경에서 원격 객체는 **어떤 노드에 존재하는지, 혹은 여러 노드에 복제되어 있는지** 객체 참조를 가진 주체와는 독립적으로 존재할 수 있다.

## **Placement Groups**

---

Placement group은 여러 노드에 걸쳐 있는 리소스 그룹을 **원자적으로(reservations are atomic)** 예약할 수 있는 기능이다.

이를 통해 Ray 태스크와 actor를:
- 서로 가깝게 PACK 하거나
- 멀리 SPREAD 해서 배치하거나

하는 스케줄링 전략을 사용 가능하다.

일반적인 사용 사례는 **gang-scheduling**(여러 태스크/actor를 한 번에 일관되게 배치해야 하는 경우)이다.

## **Environment Dependencies**

---

Ray가 원격 머신에서 태스크와 actor를 실행할 때에는, 해당 코드가 필요로 하는 환경 의존성(예: Python 패키지, 로컬 파일, 환경 변수 등)도 원격 머신에 준비되어 있어야 한다.

이 문제를 해결하기 위해 다음과 같은 방법을 사용할 수 있다:
1. Ray Cluster Launcher 를 사용하여 클러스터에 필요한 의존성을 미리 준비해두기
1. Ray Runtime Environments 를 사용하여 실행 시 동적으로 의존성을 설치하기

---

## 운영 관점에서 남긴 결론

상태가 없는 병렬 함수는 Task, 상태를 가진 장기 실행 워커는 Actor, 큰 데이터 전달은 ObjectRef로 모델링한다. 설정값을 외우기보다 제어면과 실행면, 워크로드 수명주기, 관측 가능한 성공 조건을 분리해 보는 것이 핵심입니다.

*실환경을 식별할 수 있는 호스트명·IP·내부 경로는 공개용 예시로 일반화했습니다.*
