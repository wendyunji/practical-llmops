> **TL;DR**  
> RayCluster CR이 Head와 Worker Pod 구성을 선언하고 KubeRay Operator가 실제 Kubernetes 리소스로 조정한다.

이 글은 Ray와 KubeRay를 실제 ML 플랫폼 관점에서 이해하기 위해 정리한 학습 기록입니다. 개념 소개에 그치지 않고, 어떤 운영 문제를 해결하는지와 확인해야 할 지점을 중심으로 구성했습니다.

**다루는 주제**: RayCluster · Kubernetes · Autoscaling · Ray Dashboard

---

## RayCluster

> Ray 클러스터를 관리하고 상호작용하는 방법

## 준비 사항 (Preparation)

---
- kubectl(>= 1.23), Helm(>= v3.4), Kind, Docker 설치
- Kubernetes 클러스터가 최소 CPU 4개, RAM 4GB 이상 확보되어 있어야 함

## Step 1: Kubernetes 클러스터 생성

---

Kind를 이용해 로컬 Kubernetes 클러스터를 생성한다.

이미 Kubernetes 클러스터가 있다면 이 단계는 생략해도 된다.

```bash
kind create cluster --image=kindest/node:v1.26.0
```

## Step 2: KubeRay Operator 배포

---

Helm 저장소에서 최신 안정(stable) 버전의 **KubeRay Operator**를 설치한다.

[[Ray] KubeRay Operator Installation](https://app.notion.com/p/Ray-KubeRay-Operator-Installation-2afaf74bab6280fea57ae99398f264bb?pvs=21)

## Step 3: RayCluster 커스텀 리소스 배포

---

KubeRay Operator가 정상적으로 실행 중이면 RayCluster를 배포할 수 있다.

다음은 KubeRay Helm chart에 있는 샘플 RayCluster CR을 설치하는 명령이다:

```bash
helm install raycluster kuberay/ray-cluster --version 1.4.2
```

### run

```powershell
$ helm install raycluster kuberay/ray-cluster --version 1.4.2 -n ray-system
NAME: raycluster
LAST DEPLOYED: Wed Nov 19 09:15:31 2025
NAMESPACE: ray-system
STATUS: deployed
REVISION: 1
TEST SUITE: None
```

RayCluster CR이 생성되면 아래 명령으로 확인할 수 있다:

```bash
kubectl get rayclusters
```

예시 출력:

```
NAME                 DESIRED WORKERS   AVAILABLE WORKERS   CPUS   MEMORY   GPUS   STATUS   AGE
raycluster-kuberay   1                 1                   2      3G       0      ready    55s
```

KubeRay Operator는 RayCluster 객체를 감지한 뒤 Ray 클러스터를 시작하기 위해

**Head Pod와 Worker Pod**를 생성한다.

RayCluster의 Pod를 보려면 다음 명령을 실행한다:

```bash
kubectl get pods --selector=ray.io/cluster=raycluster-kuberay
```

예시:

```
NAME                                          READY   STATUS    RESTARTS   AGE
raycluster-kuberay-head                       1/1     Running   0          XXs
raycluster-kuberay-worker-workergroup-xvfkr   1/1     Running   0          XXs
```

Pod가 `Running` 상태가 될 때까지 기다린다.

Pending 상태로 멈춰 있으면 `kubectl describe pod`로 이유를 확인하고

Docker 리소스 제한이 충족되는지 확인한다.

### (✨추가) head node RAM Upgrade

```powershell
# 1. RayCluster 리소스 패치
kubectl patch raycluster raycluster-kuberay -n ray-system --type='json' -p='[
  {"op": "replace", "path": "/spec/headGroupSpec/template/spec/containers/0/resources/limits/memory", "value": "4G"},
  {"op": "replace", "path": "/spec/headGroupSpec/template/spec/containers/0/resources/requests/memory", "value": "4G"}
]'

# 2. Head pod 강제 재시작 (자동 재시작 안되면)
kubectl delete pod -n ray-system raycluster-kuberay-head-5fzjq

# 3. 새 pod이 Ready 될 때까지 대기
kubectl wait --for=condition=ready pod -l ray.io/node-type=head -n ray-system --timeout=120s

# 4. 확인
kubectl get raycluster -n ray-system
# MEMORY: 3G → 5G (Head 4G + Worker 1G)
```

## Step 4: RayCluster에서 애플리케이션 실행

---

### 방법 1: Head Pod 안에서 Ray Job 실행

가장 쉬운 실험 방법은 Head Pod 내부에서 직접 Ray 코드를 실행하는 것이다.

먼저 RayCluster의 Head Pod 이름을 가져온다:

```bash
export HEAD_POD=$(kubectl get pods --selector=ray.io/node-type=head -o custom-columns=POD:metadata.name --no-headers)
echo $HEAD_POD
```

### run

```powershell
$ export HEAD_POD=$(kubectl get pods --selector=ray.io/node-type=head -n ray-system -o custom-columns=POD:metadata.name --no-headers)
$ echo $HEAD_POD
raycluster-kuberay-head-5fzjq
```

Ray 클러스터 리소스를 출력해보자:

```bash
kubectl exec -it $HEAD_POD -- python -c "import ray; ray.init(); print(ray.cluster_resources())"
```

### run

```powershell
$ kubectl exec -it $HEAD_POD -n ray-system -- python -c "import ray; ray.init(); print(ray.cluster_resources())"
2025-11-18 16:18:44,376	INFO worker.py:1554 -- Using address 127.0.0.1:6379 set in the environment variable RAY_ADDRESS
2025-11-18 16:18:44,377	INFO worker.py:1694 -- Connecting to existing Ray cluster at address: <node-ip>:6379...
2025-11-18 16:18:44,392	INFO worker.py:1879 -- Connected to Ray cluster. View the dashboard at <node-ip>:8265
{'memory': 3000000000.0, 'node:<node-ip>': 1.0, 'object_store_memory': 371129548.0, 'CPU': 2.0, 'node:__internal_head__': 1.0, 'node:<node-ip>': 1.0}
```

예시 출력:

```
Connected to Ray cluster...
{'CPU': 2.0, 'memory': 3000000000.0, ...}
```

### 방법 2: Ray Job Submission SDK 사용 (헤드 Pod에 들어갈 필요 없음)

이 방법은 Head Pod에 직접 접속하지 않아도 된다.

Ray Dashboard가 Job 요청을 받는 포트를 통해 Job을 제출한다.

Ray Head Pod를 노출하는 서비스를 확인한다:

```bash
kubectl get service raycluster-kuberay-head-svc
```

### run

```powershell
$ kubectl get service raycluster-kuberay-head-svc -n ray-system
NAME                          TYPE        CLUSTER-IP   EXTERNAL-IP   PORT(S)                                         AGE
raycluster-kuberay-head-svc   ClusterIP   None         <none>        10001/TCP,8265/TCP,6379/TCP,8080/TCP,8000/TCP   11m
```

예:

```
NAME                          TYPE        CLUSTER-IP    EXTERNAL-IP   PORT(S)                         AGE
raycluster-kuberay-head-svc   ClusterIP   None          <none>        10001/TCP,8265/TCP,6379/TCP...  57s
```

Dashboard 포트(기본 8265)를 포트포워딩한다:

```bash
kubectl port-forward service/raycluster-kuberay-head-svc 8265:8265 > /dev/null &
```

→ virturalservice 및 nginx-proxy 설정 [[Ray] CLI용 Virtual Service 설정](https://app.notion.com/p/Ray-CLI-Virtual-Service-2b1af74bab628016916bd79e5cf1866c?pvs=21)

이제 Job을 제출할 수 있다:

```bash
ray job submit --address http://localhost:8265 -- python -c "import ray; ray.init(); print(ray.cluster_resources())"
```

성공 예시 로그:

```
Job 'raysubmit_...' submitted successfully
{'CPU': 2.0, 'memory': ... }
Job 'raysubmit_...' succeeded
```

### (✨추가) 로컬에 가상환경 기반 ray cli 설치 & 환경변수 설정, 및 job 제출

```powershell
# ray cli 설치
python3 -m venv ~/.venv/ray-cli
source ~/.venv/ray-cli/bin/activate

pip install "ray[default]"

# 도메인 환경변수 설정
vi ~/.venv/ray-cli/bin/activate

export RAY_ADDRESS="https://ray.example.com/ray-api" # 마지막 한줄 추가

source ~/.venv/ray-cli/bin/activate # 저장 후

# 실행
$ ray job submit \
  --address https://ray.example.com/ray-api \
  -- python -c "import ray; ray.init(); print(ray.cluster_resources())"

# adress 환경변수 저장 후 호출 방법!
ray job submit -- python -c "import ray; ray.init(); print(ray.cluster_resources())"

# 결과
~/.venv/ray-cli/lib/python3.9/site-packages/urllib3/__init__.py:35: NotOpenSSLWarning: urllib3 v2 only supports OpenSSL 1.1.1+, currently the 'ssl' module is compiled with 'LibreSSL 2.8.3'. See: https://github.com/urllib3/urllib3/issues/3020
  warnings.warn(
Job submission server address: https://ray.example.com/ray-api

-------------------------------------------------------
Job 'raysubmit_5DZwL8CkTGtsrcLS' submitted successfully
-------------------------------------------------------

Next steps
  Query the logs of the job:
    ray job logs raysubmit_5DZwL8CkTGtsrcLS
  Query the status of the job:
    ray job status raysubmit_5DZwL8CkTGtsrcLS
  Request the job to be stopped:
    ray job stop raysubmit_5DZwL8CkTGtsrcLS

Tailing logs until the job exits (disable with --no-wait):
2025-11-19 15:49:52,569	INFO job_manager.py:531 -- Runtime env is setting up.
2025-11-19 15:49:53,980	INFO worker.py:1554 -- Using address <node-ip>:6379 set in the environment variable RAY_ADDRESS
2025-11-19 15:49:53,980	INFO worker.py:1694 -- Connecting to existing Ray cluster at address: <node-ip>:6379...
2025-11-19 15:49:53,990	INFO worker.py:1879 -- Connected to Ray cluster. View the dashboard at <node-ip>:8265
{'node:__internal_head__': 1.0, 'node:<node-ip>': 1.0, 'object_store_memory': 1199976038.0, 'CPU': 2.0, 'memory': 5000000000.0, 'node:<node-ip>': 1.0}

------------------------------------------
Job 'raysubmit_5DZwL8CkTGtsrcLS' succeeded
------------------------------------------
```

## Step 5: Ray Dashboard 접근

---

[[TS] Virtualservice를 통해서 특정 엔드 포인트에 정적 웹페이지 호출 가능하게 하기](https://app.notion.com/p/TS-Virtualservice-2b0af74bab6280b28126d008a99b08b6?pvs=21)

브라우저에서 다음 주소로 접속한다:

```
http://127.0.0.1:8265
```

[https://ray.example.com/ray](https://ray.example.com/ray)

Dashboard의 **Recent Jobs** 패널에서 방금 제출한 Job을 확인할 수 있다.

## Step 6: 정리(Cleanup)

---

*→ 이건 현재 환경에서는 안해도 되는 부분이라서 안함..*

포트포워딩을 종료한다:

```bash
killall kubectl
```

Kind 클러스터 삭제:

```bash
kind delete cluster
```

---

## 운영 관점에서 남긴 결론

RayCluster CR이 Head와 Worker Pod 구성을 선언하고 KubeRay Operator가 실제 Kubernetes 리소스로 조정한다. 설정값을 외우기보다 제어면과 실행면, 워크로드 수명주기, 관측 가능한 성공 조건을 분리해 보는 것이 핵심입니다.

*실환경을 식별할 수 있는 호스트명·IP·내부 경로는 공개용 예시로 일반화했습니다.*
