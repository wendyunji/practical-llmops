> **TL;DR**  
> 끝이 있는 학습·배치 작업은 RayJob으로 제출하고 shutdownAfterJobFinishes와 TTL로 리소스 회수를 자동화한다.

이 글은 Ray와 KubeRay를 실제 ML 플랫폼 관점에서 이해하기 위해 정리한 학습 기록입니다. 개념 소개에 그치지 않고, 어떤 운영 문제를 해결하는지와 확인해야 할 지점을 중심으로 구성했습니다.

**다루는 주제**: RayJob · KubeRay · Batch · Lifecycle

---

## RayJob

## 사전 요구사항

---
- KubeRay v0.6.0 이상
- 버전 호환성:
  - KubeRay v0.6.0 또는 v1.0.0 → Ray 1.10 이상
  - KubeRay v1.1.1 이상 권장 → Ray 2.8.0 이상

## RayJob이란 무엇인가?

---

RayJob은 두 가지를 관리하는 Kubernetes Custom Resource(CRD)이다.
1. RayCluster
  - RayCluster 커스텀 리소스는 Ray 클러스터의 모든 파드를 관리하며, 헤드 파드와 여러 워커 파드를 포함한다.
1. Job
  - RayCluster에 Ray job을 제출하기 위해 ray job submit 을 실행하는 Kubernetes Job을 말한다.

## RayJob이 제공하는 기능

---

RayJob을 사용하면 KubeRay가 **자동으로 RayCluster를 생성하고**, 클러스터가 준비되면 **job을 자동 제출**한다.

또한 RayJob 설정에 따라 Ray job이 완료된 후 **RayCluster를 자동 삭제하도록 구성할 수도 있다.**

다음 내용을 더 잘 이해하려면 아래 차이를 알아야 한다:
- **RayJob** : KubeRay가 제공하는 Kubernetes 커스텀 리소스 정의(CRD).
- **Ray job** : 원격 Ray 클러스터에서 실행 가능한 패키징된 Ray 애플리케이션.
- Submitter : Ray job을 RayCluster에 제출하기 위해 ray job submit 을 실행하는 Kubernetes Job.

## RayJob 구성 (RayJob Configuration)

---

### RayCluster 구성
- rayClusterSpec – Ray job을 실행할 RayCluster 커스텀 리소스를 정의한다.
- clusterSelector – 새로운 클러스터를 만들지 않고 기존 RayCluster 커스텀 리소스를 사용해 Ray job을 실행한다. 예시 구성은 ray-job.use-existing-raycluster.yaml 를 참고하라.

### Ray job 구성
- entrypoint – Submitter가 ray job submit --address ... --submission-id ... -- $entrypoint 형태로 Ray job을 RayCluster에 제출한다.
- runtimeEnvYAML (선택) – Ray job이 실행되기 위해 필요한 의존성(파일, 패키지, 환경 변수 등)을 기술한 runtime environment. 멀티라인 YAML 문자열로 제공된다. 예시: `spec:
runtimeEnvYAML: |
pip:
- requests==2.26.0
- pendulum==2.1.2
env_vars:
KEY: "VALUE"` 더 자세한 내용은 [Runtime Environments](https://docs.ray.io/en/latest/cluster/kubernetes/getting-started/rayjob-quick-start.html) (KubeRay 버전 1.0.0부터 지원) 참조.
- jobId (선택) – Ray job의 submission ID를 정의한다. 제공하지 않으면 KubeRay가 자동 생성한다. submission ID에 대한 자세한 내용은 Ray Jobs CLI API Reference 를 참조하라.
- metadata (선택) – -metadata-json 옵션에 대한 자세한 내용은 Ray Jobs CLI API Reference 를 참조하라.
- entrypointNumCpus / entrypointNumGpus / entrypointResources (선택) – 더 자세한 내용은 Ray Jobs CLI API Reference 를 참조하라.
- backoffLimit (선택, 버전 1.2.0에서 추가) – 이 RayJob을 실패로 표시하기 전에 재시도할 횟수를 지정한다. 각 재시도는 새로운 RayCluster를 생성한다. 기본값은 0이다.

### 제출(Submission) 구성
- submissionMode (선택) – RayJob이 Ray job을 RayCluster에 어떻게 제출할지를 지정한다. 가능한 값은 세 가지이며 기본값은 K8sJobMode 이다.
  - K8sJobMode : KubeRay operator가 Ray job을 제출하기 위한 submitter Kubernetes Job을 생성한다.
  - HTTPMode : KubeRay operator가 RayCluster에 요청을 보내 Ray job을 생성한다.
  - InteractiveMode : KubeRay operator는 사용자가 RayCluster에 job을 제출할 때까지 기다린다. 이 모드는 현재 알파 상태이고, KubeRay kubectl 플러그인이 이 모드에 의존한다.
  - SidecarMode : KubeRay operator가 Ray job 제출을 위해 Ray head Pod에 컨테이너를 주입한다. 이 모드는 clusterSelector , submitterPodTemplate , submitterConfig 를 지원하지 않으며, head Pod의 restart policy가 Never 여야 한다.
- submitterPodTemplate (선택) – submitter Kubernetes Job을 위한 Pod 템플릿을 정의한다. 이 필드는 submissionMode 가 "K8sJobMode" 일 때만 유효하다.
  - RAY_DASHBOARD_ADDRESS – KubeRay operator가 이 환경 변수를 submitter Pod에 주입한다. 값은 $HEAD_SERVICE:$DASHBOARD_PORT 이다.
  - RAY_JOB_SUBMISSION_ID – KubeRay operator가 이 환경 변수를 submitter Pod에 주입한다. 값은 RayJob의 Status.JobId 이다. 예시: `ray job submit --address=http://$RAY_DASHBOARD_ADDRESS --submission-id=$RAY_JOB_SUBMISSION_ID …` 더 자세한 내용은 ray-job.sample.yaml 참조.
- submitterConfig (선택) – submitter Kubernetes Job에 대한 추가 설정이다.
  - backoffLimit (선택, 버전 1.2.0에서 추가): submitter Job을 실패로 표시하기 전 재시도 횟수. 기본값은 2이다.

### 자동 리소스 정리 (Automatic resource cleanup)
- shutdownAfterJobFinishes (선택) – Ray job 완료 후 RayCluster를 회수할 것인지 결정한다. 기본값은 false이다.
- ttlSecondsAfterFinished (선택) – shutdownAfterJobFinishes=true 일 때만 동작한다. KubeRay operator는 Ray job이 끝난 뒤 ttlSecondsAfterFinished 초가 지나면 RayCluster와 submitter를 삭제한다. 기본값은 0이다.
- activeDeadlineSeconds (선택) – RayJob이 지정된 activeDeadlineSeconds 내에 JobDeploymentStatus 를 Complete 또는 Failed 로 전환하지 못하면, KubeRay operator는 이유를 DeadlineExceeded 로 하여 JobDeploymentStatus 를 Failed 로 전환한다.
- DELETE_RAYJOB_CR_AFTER_JOB_FINISHES (선택, 버전 1.2.0에서 추가) – 이 환경 변수는 RayJob 리소스가 아니라 KubeRay operator 에 대해 설정해야 한다. 이 변수를 true로 설정하고 동시에 shutdownAfterJobFinishes=true 로 설정했을 경우, RayJob 커스텀 리소스 자체도 삭제된다. KubeRay는 RayJob이 생성한 모든 리소스를 삭제하며, 여기에는 Kubernetes Job도 포함된다.

### 기타 (Others)
- suspend (선택) – suspend=true 이면 KubeRay는 RayCluster와 submitter 둘 다 삭제한다. Kueue 또한 이 필드를 변경하여 스케줄링 전략을 구현하므로, RayJob을 Kueue로 스케줄링하는 경우 이 필드를 수동으로 업데이트하는 건 피해야 한다.
- deletionStrategy (선택, v1.5.0 알파) – RayJob이 종료 상태(terminal state)에 도달한 후 자동 정리를 구성한다. 이 필드는 RayJobDeletionPolicy feature gate가 활성화되어 있어야 한다. 서로 배타적인 스타일 두 가지가 지원된다:
  - Rules-based (권장) : deletionRules 를 특정 조건에 의해 트리거되는 삭제 동작 리스트로 정의한다. 각 rule은:
    - policy : 삭제 동작 – DeleteCluster , DeleteWorkers , DeleteSelf , DeleteNone 중 하나
    - condition : 언제 삭제를 트리거할지, jobStatus (SUCCEEDED 또는 FAILED) 및 선택적으로 ttlSeconds 지연 포함 이 방식은 다단계 정리 전략을 가능하게 한다 (예: 성공 시 즉시 워커 삭제 → 300초 후 클러스터 삭제). Rules-based 모드는 `shutdownAfterJobFinishes` 및 전역 `ttlSecondsAfterFinished`와 호환되지 않는다. 대신 rule별 `condition.ttlSeconds`를 사용해야 한다. 예시 구성은 ray-job.deletion-rules.yaml 참조.
  - Legacy (Deprecated) : onSuccess 및 onFailure 정책을 정의하는 방식. 이 방식은 deprecated이며 v1.6.0에서 제거될 예정이다. deletionRules 방식으로 마이그레이션하는 것이 강력히 권장된다. Legacy 모드는 shutdownAfterJobFinishes 및 전역 ttlSecondsAfterFinished 와 함께 사용할 수 있다.
- 자세한 API 명세는 KubeRay API Reference 참조.

## 예제: 간단한 Ray job 실행하기

---

### Step 1: Kind로 Kubernetes 클러스터 생성

```
kind create cluster --image=kindest/node:v1.26.0
```

### **Step 2: KubeRay operator 설치**

Helm으로 최신 버전 설치.

→ 여기까지는 완료되어있음.

### Step 3: RayJob 설치

```
kubectl apply -f https://raw.githubusercontent.com/ray-project/kuberay/v1.4.2/ray-operator/config/samples/ray-job.sample.yaml
```

### run

```powershell
$ kubectl apply -f https://raw.githubusercontent.com/ray-project/kuberay/v1.4.2/ray-operator/config/samples/ray-job.sample.yaml -n ray-system
rayjob.ray.io/rayjob-sample created
configmap/ray-job-code-sample created
```

### Step 4: Kubernetes 클러스터 상태 확인

**4.1 RayJob 목록 조회**

```
kubectl get rayjob
```

### run

```powershell
$ kubectl get rayjob -n ray-system
NAME            JOB STATUS   DEPLOYMENT STATUS   RAY CLUSTER NAME      START TIME             END TIME   AGE
rayjob-sample                Initializing        rayjob-sample-xj27l   2025-11-25T07:02:30Z              18s
$ kubectl get rayjob -n ray-system
NAME            JOB STATUS   DEPLOYMENT STATUS   RAY CLUSTER NAME      START TIME             END TIME   AGE
rayjob-sample   SUCCEEDED    Running             rayjob-sample-xj27l   2025-11-25T07:02:30Z              2m24s
```

**4.2 RayCluster 목록 조회**

```
kubectl get raycluster
```

### run

```powershell
$ k get rayclusters.ray.io -n ray-system
NAME                  DESIRED WORKERS   AVAILABLE WORKERS   CPUS   MEMORY   GPUS   STATUS   AGE
raycluster-kuberay    1                 1                   2      5G       0      ready    6d23h
rayjob-sample-xj27l   1                 1                   400m   0        0      ready    16h
```

**4.3 Pod 확인**

```
kubectl get pods
```

### run

```powershell
$ k get pods -n ray-system
NAME                                           READY   STATUS      RESTARTS        AGE
kuberay-operator-6d74c6dc6c-pm9dq              1/1     Running     0               7d11h
ray-dashboard-nginx-76cbd56d4-hjd7l            1/1     Running     1 (5d23h ago)   6d17h
ray-dashboard-nginx-76cbd56d4-mtm9p            1/1     Running     1 (5d23h ago)   6d17h
raycluster-kuberay-head-7b6ft                  1/1     Running     0               5d23h
raycluster-kuberay-workergroup-worker-6lwzg    1/1     Running     3 (5d23h ago)   6d23h
rayjob-sample-86d6n                            0/1     Completed   0               16h
rayjob-sample-xj27l-head-4bvtf                 1/1     Running     0               16h
rayjob-sample-xj27l-small-group-worker-lwkb8   1/1     Running     0               16h
```

**4.4 RayJob 상태 확인**

```
kubectl get rayjobs.ray.io rayjob-sample -o jsonpath='{.status.jobStatus}'
kubectl get rayjobs.ray.io rayjob-sample -o jsonpath='{.status.jobDeploymentStatus}'
```

### run

```powershell
$ kubectl get rayjobs.ray.io rayjob-sample -o jsonpath='{.status.jobStatus}' -n ray-system
SUCCEEDED[user@admin-host ~]$ kubectl get rayjobs.ray.io rayjob-sample -o jsonpath='{.status.jobDeploymentStatus}' -n ray-system
Complete
```
- KubeRay 오퍼레이터는 rayClusterSpec 을 기반으로 RayCluster 커스텀 리소스 를 생성하고, Ray job을 해당 RayCluster에 제출하기 위해 submitter용 Kubernetes Job 도 함께 생성한다.
- 이 예제에서 entrypoint 는 python /home/ray/samples/sample_code.py 이며, sample_code.py 는 Kubernetes ConfigMap에 저장된 Python 스크립트 로, RayCluster의 head Pod에 마운트 되어 있다. code`[user@admin-host ~]$ k describe configmaps -n ray-system ray-job-code-sample
Name: ray-job-code-sample
Namespace: ray-system
Labels: <none>
Annotations: <none>

Data
====
sample_code.py:
----
import ray
import os
import requests

ray.init()

@ray.remote
class Counter:
def __init__(self):
# Used to verify runtimeEnv
self.name = os.getenv("counter_name")
assert self.name == "test_counter"
self.counter = 0

def inc(self):
self.counter += 1

def get_counter(self):
return "{} got {}".format(self.name, self.counter)

counter = Counter.remote()

for _ in range(5):
ray.get(counter.inc.remote())
print(ray.get(counter.get_counter.remote()))

# Verify that the correct runtime env was used for the job.
assert requests.__version__ == "2.26.0"

BinaryData
====

Events: <none>`
- shutdownAfterJobFinishes 기본값이 false 이기 때문에, Ray job 실행이 끝나도 KubeRay 오퍼레이터는 RayCluster나 submitter Job을 삭제하지 않는다.

### Step 5: Ray job 출력 확인

```
kubectl logs -l=job-name=rayjob-sample
```

### run

```powershell
$ kubectl logs -l=job-name=rayjob-sample -n ray-system
2025-11-24 23:08:52,168	INFO worker.py:1694 -- Connecting to existing Ray cluster at address: <node-ip>:6379...
2025-11-24 23:08:52,180	INFO worker.py:1879 -- Connected to Ray cluster. View the dashboard at <node-ip>:8265
test_counter got 1
test_counter got 2
test_counter got 3
test_counter got 4
test_counter got 5
2025-11-24 23:08:57,932	SUCC cli.py:65 -- -----------------------------------
2025-11-24 23:08:57,932	SUCC cli.py:66 -- Job 'rayjob-sample-zcvxc' succeeded
2025-11-24 23:08:57,932	SUCC cli.py:67 -- -----------------------------------
```

### Step 6: RayJob 삭제

```
kubectl delete -f https://raw.githubusercontent.com/ray-project/kuberay/v1.4.2/ray-operator/config/samples/ray-job.sample.yaml
```

### run

```powershell
$ kubectl delete -f https://raw.githubusercontent.com/ray-project/kuberay/v1.4.2/ray-operator/config/samples/ray-job.sample.yaml -n ray-system
rayjob.ray.io "rayjob-sample" deleted
configmap "ray-job-code-sample" deleted
```

### Step 7: `shutdownAfterJobFinishes`를 true로 설정한 RayJob 생성

```
kubectl apply -f https://raw.githubusercontent.com/ray-project/kuberay/v1.4.2/ray-operator/config/samples/ray-job.shutdown.yaml
```

### run

```powershell
$ kubectl apply -f https://raw.githubusercontent.com/ray-project/kuberay/v1.4.2/ray-operator/config/samples/ray-job.shutdown.yaml -n ray-system
rayjob.ray.io/rayjob-sample-shutdown created
configmap/ray-job-code-sample created

# Initialize 할때
$ k get all -n ray-system
NAME                                                        READY   STATUS     RESTARTS     AGE
pod/kuberay-operator-6d74c6dc6c-pm9dq                       1/1     Running    0            7d11h
pod/ray-dashboard-nginx-76cbd56d4-hjd7l                     1/1     Running    1 (6d ago)   6d17h
pod/ray-dashboard-nginx-76cbd56d4-mtm9p                     1/1     Running    1 (6d ago)   6d17h
pod/raycluster-kuberay-head-7b6ft                           1/1     Running    0            6d
pod/raycluster-kuberay-workergroup-worker-6lwzg             1/1     Running    3 (6d ago)   6d23h
pod/rayjob-sample-shutdown-rmqx4-head-bhfzm                 0/1     Running    0            6s
pod/rayjob-sample-shutdown-rmqx4-small-group-worker-5vvjx   0/1     Init:0/1   0            6s

NAME                                            TYPE        CLUSTER-IP       EXTERNAL-IP   PORT(S)                                         AGE
service/kuberay-operator                        ClusterIP   <node-ip>      <none>        8080/TCP                                        7d11h
service/ray-dashboard-nginx                     ClusterIP   <node-ip>   <none>        80/TCP                                          6d17h
service/raycluster-kuberay-head-svc             ClusterIP   None             <none>        10001/TCP,8265/TCP,6379/TCP,8080/TCP,8000/TCP   6d23h
service/rayjob-sample-shutdown-rmqx4-head-svc   ClusterIP   None             <none>        10001/TCP,8265/TCP,6379/TCP,8080/TCP            6s
# ... verbose output omitted ...
replicaset.apps/ray-dashboard-nginx-778b7f796d   0         0         0       6d17h

NAME                               STATUS     COMPLETIONS   DURATION   AGE
job.batch/rayjob-sample-shutdown   Complete   1/1           22s        52s

NAME                                   DESIRED WORKERS   AVAILABLE WORKERS   CPUS   MEMORY   GPUS   STATUS   AGE
raycluster.ray.io/raycluster-kuberay   1                 1                   2      5G       0      ready    6d23h

NAME                                   JOB STATUS   DEPLOYMENT STATUS   RAY CLUSTER NAME               START TIME             END TIME               AGE
rayjob.ray.io/rayjob-sample-shutdown   SUCCEEDED    Complete            rayjob-sample-shutdown-rmqx4   2025-11-25T23:58:25Z   2025-11-25T23:59:11Z   76s
```
- shutdownAfterJobFinishes: true
- ttlSecondsAfterFinished: 10

→ Ray job 종료 후 **10초 뒤 RayCluster 자동 삭제**

Submitter Job은 로그가 있으므로 삭제되지 않음.

### Step 8: RayJob 상태 확인

```
kubectl get rayjobs.ray.io rayjob-sample-shutdown -o jsonpath='{.status.jobDeploymentStatus}'
kubectl get rayjobs.ray.io rayjob-sample-shutdown -o jsonpath='{.status.jobStatus}'
```

### run

```powershell
$ kubectl get rayjobs.ray.io rayjob-sample-shutdown -o jsonpath='{.status.jobDeploymentStatus}' -n ray-system
Complete[user@admin-host ~]$ kubectl get rayjobs.ray.io rayjob-sample-shutdown -o jsonpath='{.status.jobStatus}' -n ray-system
SUCCEEDED
```

### Step 9: KubeRay operator가 RayCluster를 삭제했는지 확인

```
kubectl get raycluster
```

### run

```powershell
SUCCEEDED[user@admin-host ~]$ k get rayclusters.ray.io -n ray-system
NAME                 DESIRED WORKERS   AVAILABLE WORKERS   CPUS   MEMORY   GPUS   STATUS   AGE
raycluster-kuberay   1                 1                   2      5G       0      ready    6d23h
```

### Step 10: 정리(Cleanup)

**10.1 RayJob 삭제**

```
kubectl delete -f https://raw.githubusercontent.com/ray-project/kuberay/v1.4.2/ray-operator/config/samples/ray-job.shutdown.yaml
```

### run

```powershell
$ kubectl delete -f https://raw.githubusercontent.com/ray-project/kuberay/v1.4.2/ray-operator/config/samples/ray-job.shutdown.yaml -n ray-system
rayjob.ray.io "rayjob-sample-shutdown" deleted
configmap "ray-job-code-sample" deleted
$ k get all -n ray-system
NAME                                              READY   STATUS    RESTARTS     AGE
pod/kuberay-operator-6d74c6dc6c-pm9dq             1/1     Running   0            7d12h
pod/ray-dashboard-nginx-76cbd56d4-hjd7l           1/1     Running   1 (6d ago)   6d17h
pod/ray-dashboard-nginx-76cbd56d4-mtm9p           1/1     Running   1 (6d ago)   6d17h
pod/raycluster-kuberay-head-7b6ft                 1/1     Running   0            6d
pod/raycluster-kuberay-workergroup-worker-6lwzg   1/1     Running   3 (6d ago)   6d23h

NAME                                  TYPE        CLUSTER-IP       EXTERNAL-IP   PORT(S)                                         AGE
service/kuberay-operator              ClusterIP   <node-ip>      <none>        8080/TCP                                        7d12h
service/ray-dashboard-nginx           ClusterIP   <node-ip>   <none>        80/TCP                                          6d18h
service/raycluster-kuberay-head-svc   ClusterIP   None             <none>        10001/TCP,8265/TCP,6379/TCP,8080/TCP,8000/TCP   6d23h

NAME                                  READY   UP-TO-DATE   AVAILABLE   AGE
deployment.apps/kuberay-operator      1/1     1            1           7d12h
deployment.apps/ray-dashboard-nginx   2/2     2            2           6d18h

NAME                                             DESIRED   CURRENT   READY   AGE
replicaset.apps/kuberay-operator-6d74c6dc6c      1         1         1       7d12h
replicaset.apps/ray-dashboard-nginx-76cbd56d4    2         2         2       6d17h
replicaset.apps/ray-dashboard-nginx-778b7f796d   0         0         0       6d18h

NAME                                   DESIRED WORKERS   AVAILABLE WORKERS   CPUS   MEMORY   GPUS   STATUS   AGE
raycluster.ray.io/raycluster-kuberay   1                 1                   2      5G       0      ready    6d23h
```

〠오퍼레이터랑 클러스터 삭제는 안함〠

**10.2 KubeRay Operator 삭제**

```
helm uninstall kuberay-operator
```

**10.3 클러스터 삭제**

```
kind delete cluster
```

---

## 운영 관점에서 남긴 결론

끝이 있는 학습·배치 작업은 RayJob으로 제출하고 shutdownAfterJobFinishes와 TTL로 리소스 회수를 자동화한다. 설정값을 외우기보다 제어면과 실행면, 워크로드 수명주기, 관측 가능한 성공 조건을 분리해 보는 것이 핵심입니다.

*실환경을 식별할 수 있는 호스트명·IP·내부 경로는 공개용 예시로 일반화했습니다.*
