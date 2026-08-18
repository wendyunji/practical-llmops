> **TL;DR**  
> 장기 실행 서빙에는 RayService가 적합하다. Serve 상태와 RayCluster 상태를 함께 관리하고 무중단 갱신 경로를 제공한다.

이 글은 Ray와 KubeRay를 실제 ML 플랫폼 관점에서 이해하기 위해 정리한 학습 기록입니다. 개념 소개에 그치지 않고, 어떤 운영 문제를 해결하는지와 확인해야 할 지점을 중심으로 구성했습니다.

**다루는 주제**: RayService · Ray Serve · Kubernetes · Zero Downtime

---

## RayService

## 준비 사항 (Prerequisites)

---
- kubectl (>= 1.23)
- Helm (>= v3.4)
- Kind, Docker 설치
- 최소 스펙: CPU 4개, RAM 4GB 이상 의 Kubernetes 클러스터
- 본 가이드는 KubeRay v1.5.0 + Ray 2.46.0 기준

## RayService란?

---

RayService는 다음 두 가지를 함께 관리하는 KubeRay의 커스텀 리소스다.
1. RayCluster
  - Kubernetes 클러스터 안에서 Ray 헤드/워커 노드를 포함한 리소스를 관리.
1. Ray Serve Applications
  - 사용자가 작성한 Ray Serve 애플리케이션(REST API, 추론 서버 등)을 관리.

## RayService가 제공하는 것

---
- Kubernetes 네이티브 Ray + Serve 관리
  - YAML 기반 Kubernetes 설정만으로 Ray 클러스터와 Serve 애플리케이션을 정의한 뒤, kubectl apply 같은 표준 명령으로 배포 및 운영할 수 있다.
- Ray Serve 애플리케이션 인플레이스 업데이트
  - RayService를 통해 Serve 애플리케이션을 중단 없이 업데이트할 수 있다.
- Ray 클러스터 무중단 업그레이드(Zero-downtime)
  - RayCluster를 무중단으로 교체/업데이트하는 기능을 지원한다.
- 고가용성(High Availability)
  - 비정상 RayCluster가 감지되면 새 클러스터를 올려 교체하는 등의 자체 복구 로직 제공.
- 참고 링크
  - https://docs.ray.io/en/latest/cluster/kubernetes/user-guides/rayservice.html#kuberay-rayservice
  - https://docs.ray.io/en/latest/cluster/kubernetes/user-guides/rayservice-high-availability.html#kuberay-rayservice-ha

## 예제: RayService로 두 개의 간단한 Ray Serve 앱 배포하기

> - /fruit/ 엔드포인트 (과일 관련 연산)
>
> - /calc/ 엔드포인트 (간단 계산기)

## Step 1: Kind로 Kubernetes 클러스터 생성

---

```bash
kind create cluster --image=kindest/node:v1.26.0
```

## Step 2: KubeRay Operator 설치

---

Helm 저장소에서 **최신 stable KubeRay Operator**를 설치한다.

→ [[Ray] KubeRay Operator Installation](https://app.notion.com/p/Ray-KubeRay-Operator-Installation-2afaf74bab6280fea57ae99398f264bb?pvs=21)

이 예제에서 사용하는 RayService 샘플 YAML은 `serveConfigV2`를 사용하며, 이는 **KubeRay v0.6.0 이상**에서 지원되는 **멀티 애플리케이션 Serve 설정 방식**이다.

## Step 3: RayService 커스텀 리소스 배포

---

```bash
kubectl apply -f https://raw.githubusercontent.com/ray-project/kuberay/v1.5.0/ray-operator/config/samples/ray-service.sample.yaml
```

### run

```powershell
$ kubectl apply -f https://raw.githubusercontent.com/ray-project/kuberay/v1.5.0/ray-operator/config/samples/ray-service.sample.yaml -n ray-system
rayservice.ray.io/rayservice-sample created
```

## Step 4: Kubernetes 리소스 상태 확인

---

**Step 4.1: RayService 목록 확인**

```bash
kubectl get rayservice
```

예시 출력:

```
NAME                SERVICE STATUS   NUM SERVE ENDPOINTS
rayservice-sample   Running          2
```
- SERVICE STATUS: Running 이고
- NUM SERVE ENDPOINTS 가 0보다 크면 → Ray Serve 애플리케이션이 정상적으로 올라온 상태.

### run

```powershell
$ k get rayservices.ray.io -n ray-system
NAME                SERVICE STATUS   NUM SERVE ENDPOINTS
rayservice-sample   Running          2
```

**Step 4.2: RayCluster 목록 확인**

```bash
kubectl get raycluster
```

예시 출력:

```
NAME                      DESIRED WORKERS   AVAILABLE WORKERS   CPUS    MEMORY   GPUS   STATUS   AGE
rayservice-sample-cxm7t   1                 1                   2500m   4Gi      0      ready    79s
```
- STATUS: ready 면 클러스터가 정상.
- RayService는 내부적으로 활성(active) RayCluster를 1개 유지한다.

### run

```powershell
$ kubectl get raycluster -n ray-system
NAME                      DESIRED WORKERS   AVAILABLE WORKERS   CPUS    MEMORY   GPUS   STATUS   AGE
raycluster-kuberay        1                 1                   2       5G       0      ready    7d6h
rayservice-sample-jj29r   1                 1                   2500m   4Gi      0      ready    100m
```

**Step 4.3: Ray Pod 목록 확인**

```bash
kubectl get pods -l=ray.io/is-ray-node=yes
```

예시 출력:

```
NAME                                               READY   STATUS    RESTARTS   AGE
rayservice-sample-cxm7t-head                       1/1     Running   0          3m5s
rayservice-sample-cxm7t-small-group-worker-8hrgg   1/1     Running   0          3m5s
```
- head Pod 1개
- worker Pod 1개
- 둘 다 Running 상태가 되어야 함.

### run

```powershell
$ kubectl get pods -l=ray.io/is-ray-node=yes -n ray-system
NAME                                               READY   STATUS    RESTARTS       AGE
raycluster-kuberay-head-7b6ft                      1/1     Running   0              6d6h
raycluster-kuberay-workergroup-worker-6lwzg        1/1     Running   3 (6d6h ago)   7d6h
rayservice-sample-jj29r-head-8hklk                 1/1     Running   0              106m
rayservice-sample-jj29r-small-group-worker-6xfnx   1/1     Running   0              106m
```

**Step 4.4: RayService Ready 조건 확인**

RayService가 요청을 받을 준비가 되었는지는 **Condition**으로 확인한다.

```bash
kubectl describe rayservices.ray.io rayservice-sample
```

예시 출력:

```
Conditions:
  Last Transition Time:  2025-06-26T13:23:06Z
  Message:               Number of serve endpoints is greater than 0
  Observed Generation:   1
  Reason:                NonZeroServeEndpoints
  Status:                True
  Type:                  Ready
```
- Type: Ready
- Status: True
- 메시지: Number of serve endpoints is greater than 0 → 최소 1개 이상의 Serve 엔드포인트가 살아 있어 요청을 처리 가능하다는 의미.

### run

```powershell
$ kubectl describe rayservices.ray.io rayservice-sample -n ray-system
Name:         rayservice-sample
Namespace:    ray-system
Labels:       <none>
Annotations:  <none>
API Version:  ray.io/v1
Kind:         RayService
Metadata:
  Creation Timestamp:  2025-11-26T04:40:23Z
  Generation:          1
  Resource Version:    235818108
  UID:                 39bea00b-9863-4f3d-b1fd-446589cb4666
Spec:
  Ray Cluster Config:
    Head Group Spec:
      Ray Start Params:
      Template:
        Spec:
          Containers:
            Image:  rayproject/ray:2.46.0
# ... verbose output omitted ...
  Observed Generation:     1
  Pending Service Status:
    Ray Cluster Status:
      Desired CPU:     0
      Desired GPU:     0
      Desired Memory:  0
      Desired TPU:     0
      Head:
  Service Status:  Running
Events:            <none>
```

**Step 4.5: Service 리소스 확인**

```bash
kubectl get services
```

예시 출력:

```
NAME                               TYPE        CLUSTER-IP      EXTERNAL-IP   PORT(S)                                         AGE
...
rayservice-sample-cxm7t-head-svc   ClusterIP   None            <none>        10001/TCP,8265/TCP,6379/TCP,8080/TCP,8000/TCP   71m
rayservice-sample-head-svc         ClusterIP   None            <none>        10001/TCP,8265/TCP,6379/TCP,8080/TCP,8000/TCP   70m
rayservice-sample-serve-svc        ClusterIP   <node-ip>   <none>        8000/TCP                                        70m
```

### run

```powershell
$ kubectl get services -n ray-system
NAME                               TYPE        CLUSTER-IP       EXTERNAL-IP   PORT(S)                                         AGE
kuberay-operator                   ClusterIP   <node-ip>      <none>        8080/TCP                                        7d18h
ray-dashboard-nginx                ClusterIP   <node-ip>   <none>        80/TCP                                          7d
raycluster-kuberay-head-svc        ClusterIP   None             <none>        10001/TCP,8265/TCP,6379/TCP,8080/TCP,8000/TCP   7d6h
rayservice-sample-head-svc         ClusterIP   None             <none>        10001/TCP,8265/TCP,6379/TCP,8080/TCP,8000/TCP   142m
rayservice-sample-jj29r-head-svc   ClusterIP   None             <none>        10001/TCP,8265/TCP,6379/TCP,8080/TCP,8000/TCP   142m
rayservice-sample-serve-svc        ClusterIP   <node-ip>     <none>        8000/TCP                                        142m
```

RayService가 Healthy 상태가 되면 **자동으로 두 가지 Service**가 생성된다:

**1)**`**rayservice-sample-head-svc**`
- 활성(active) RayCluster의 head Pod 를 가리킨다.
- 주로 아래 용도로 사용:
  - Ray Dashboard 접속 (포트 8265)
  - 내부에서 Ray 클러스터에 접속 ( RAY_ADDRESS 등)

**2)**`**rayservice-sample-serve-svc**`
- Ray Serve의 HTTP 서버(기본 포트 8000 )를 노출하는 서비스.
- 이 엔드포인트로 아래와 같은 요청을 보내게 된다:
  - REST API 호출
  - 모델 추론 요청
  - 내부 서비스 간 gRPC/HTTP 호출 등

## Step 5: Ray Serve 애플리케이션 상태 확인 (Dashboard)

---

Ray Dashbaord의 Serve 페이지를 통해 현재 올라가 있는 Serve 앱들을 확인할 수 있다.

먼저 head service 포트를 로컬로 포워딩한다:

```bash
kubectl port-forward svc/rayservice-sample-head-svc 8265:8265
```

브라우저에서 접속:

```
http://localhost:8265/#/serve
```
- Serve 페이지에서:
  - Applications , Deployments , Replicas 상태
  - /fruit/ , /calc/ 라우트 존재 여부
  - 에러 로그 등 확인 가능

### (✨추가) Nodeport로 포트포워딩 하기
- 생성된 pod에 접근할 수 있는 새 NodePort 생성 단, 파드가 새로 생기거나 바뀔때마다 새롭게 만들어줘야함. `apiVersion: v1
kind: Service
metadata:
name: rayservice-dashboard-nodeport
namespace: ray-system
spec:
type: NodePort
selector:
ray.io/cluster: rayservice-sample-jj29r
ray.io/node-type: head
ports:
- name: dashboard
port: 8265
targetPort: 8265
nodePort: 30265 # 브라우저에서 http://<node-ip>:30265 로 접속
- name: serve
port: 8000
targetPort: 8000
nodePort: 30800 # Serve 애플리케이션 엔드포인트`
- dashboard 접속 `http://<node-ip>:30265/#/serve`

## Step 6: Kubernetes 서비스 경유로 Serve 앱 호출

---

여기서는 **클러스터 내부 Pod(curl)** 에서

`rayservice-sample-serve-svc` 서비스를 직접 호출하는 방식으로 테스트한다.

**Step 6.1: curl Pod 생성**

```bash
kubectl run curl --image=radial/busyboxplus:curl -i --tty
```

이미 curl Pod가 있다면:

```bash
kubectl exec -it <curl-pod> -- sh
```

**→ 어짜피 노드포트로 뚫어서 외부에서 테스트 가능**

**Step 6.2: fruit stand 앱 호출**

```bash
curl -X POST \
  -H 'Content-Type: application/json' \
  rayservice-sample-serve-svc:8000/fruit/ \
  -d '["MANGO", 2]'
```

예상 출력:

```
6
```

### run, 회사 망 접근되는 pc에서

```powershell
$ curl -X POST \
  -H 'Content-Type: application/json' \
  http://<node-ip>:30800/fruit/ \
  -d '["MANGO", 2]'
6%
```

**Step 6.3: calculator 앱 호출**

```bash
curl -X POST \
  -H 'Content-Type: application/json' \
  rayservice-sample-serve-svc:8000/calc/ \
  -d '["MUL", 3]'
```

예상 출력:

```
"15 pizzas please!"
```

### run, 회사 망 접근되는 pc에서

```powershell
$ curl -X POST \
  -H 'Content-Type: application/json' \
  http://<node-ip>:30800/calc/ \
  -d '["MUL", 3]'
15 pizzas please!%
```

## Step 7: 정리(Cleanup)

---

**Step 7.1: RayService 삭제**

```bash
kubectl delete -f https://raw.githubusercontent.com/ray-project/kuberay/v1.5.0/ray-operator/config/samples/ray-service.sample.yaml
```

### run

```powershell
$ kubectl delete -f https://raw.githubusercontent.com/ray-project/kuberay/v1.5.0/ray-operator/config/samples/ray-ervice.sample.yaml -n ray-system
rayservice.ray.io "rayservice-sample" deleted
```

**Step 7.2: KubeRay Operator 제거**

```bash
helm uninstall kuberay-operator
```

**Step 7.3:** **curl Pod 삭제**

```bash
kubectl delete pod curl
```

---

## 운영 관점에서 남긴 결론

장기 실행 서빙에는 RayService가 적합하다. Serve 상태와 RayCluster 상태를 함께 관리하고 무중단 갱신 경로를 제공한다. 설정값을 외우기보다 제어면과 실행면, 워크로드 수명주기, 관측 가능한 성공 조건을 분리해 보는 것이 핵심입니다.

*실환경을 식별할 수 있는 호스트명·IP·내부 경로는 공개용 예시로 일반화했습니다.*
