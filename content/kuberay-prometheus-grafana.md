> **TL;DR**  
> 클러스터 공용 Prometheus 스택은 하나만 운영하고, Ray 워크로드별 PodMonitor와 ServiceMonitor를 분리한다.

이 글은 Ray와 KubeRay를 실제 ML 플랫폼 관점에서 이해하기 위해 정리한 학습 기록입니다. 개념 소개에 그치지 않고, 어떤 운영 문제를 해결하는지와 확인해야 할 지점을 중심으로 구성했습니다.

**다루는 주제**: KubeRay · Prometheus · Grafana · Monitoring

---

## **Prometheus 및 Grafana 사용하기**

> Kubernetes에서 Prometheus와 Grafana 사용 경험이 없다면, 이 [YouTube 플레이리스트](https://www.youtube.com/playlist?list=PLy7NrYWoggjxCF3av5JKwyG7FFF9eLeL4)를 참고하세요.

## **Preparation (준비)**

---

[KubeRay 저장소](https://github.com/ray-project/kuberay)를 클론하고 master 브랜치를 체크아웃합니다. 이 튜토리얼에서는 저장소 내 여러 파일이 필요합니다.

## **Step 1: Kind로 Kubernetes 클러스터 생성**

---

```
kind create cluster
```

## **Step 2: Helm chart를 통해 Kubernetes Prometheus Stack 설치**

---

```
# Path: kuberay/
./install/prometheus/install.sh --auto-load-dashboard true

# 설치 확인
kubectl get all -n prometheus-system
```

(출력 일부)

```
NAME                                                  READY   UP-TO-DATE   AVAILABLE   AGE
deployment.apps/prometheus-grafana                    1/1     1            1           46s
deployment.apps/prometheus-kube-prometheus-operator   1/1     1            1           46s
deployment.apps/prometheus-kube-state-metrics         1/1     1            1           46s
```

### **run <<이대로 하면 !큰!일!남!>>**

나도 진짜 미안하다 ㅠㅠ

이론상은 안전해야 하는데, 실제로는 “kube-prometheus-stack 두 번 설치”가 생각보다 여러 군데서 잘 꼬여…

지금 상황을 최대한 현실적으로 추정해서 “왜 기존 프로메테우스/그라파나에 영향이 갔는지”를 정리해볼게.

---

## 1. 원래 내가 가정하고 있었던 그림

내 머릿속 그림은 이거였어:
- 기존 모니터링 스택
  - namespace: monitoring
  - release: prometheus-stack (대충 이런 느낌)
  - chart: kube-prometheus-stack (버전 A)
- 새로 깐 Ray 튜토리얼용 스택
  - namespace: prometheus-system
  - release: prometheus
  - chart: kube-prometheus-stack (버전 B)

Helm 입장에서는:
- 서로 다른 namespace
- 서로 다른 release 이름

→ **리소스 이름이 겹치지 않으면** “각자 자기 것만 관리”해야 해서, 이론상 기존 거에 손 안 댄다고 본 거지.

근데 현실은 **kube-prometheus-stack이 클러스터 전체에 영향을 주는 구성 요소**가 섞여 있어서, 두 번째를 깔면 “완전히 독립”이 안 되는 구석들이 있어.

---

## 2. 실제로 문제가 났을 법한 지점들

정확한 에러 메시지를 내가 못 본 상태라 100% 딱 집어 말할 수는 없지만,

**“기존 프로메테우스/그라파나가 이상해질 수 있는 대표적인 원인”**들은 이렇다:

### 2-1. CRD(커스텀 리소스 정의) 재설치 / 버전 충돌

`kube-prometheus-stack`은 이런 CRD들을 설치해:
- prometheuses.monitoring.coreos.com
- servicemonitors.monitoring.coreos.com
- podmonitors.monitoring.coreos.com
- prometheusrules.monitoring.coreos.com
- …

이 CRD들은 **클러스터 전역(ClusterScoped)** 이라 한 번만 존재해야 하는데,

두 번째 스택을 다른 버전으로 깔면서:
- CRD 스키마가 살짝 바뀐 상태로 덮어써짐
- 기존 Prometheus Operator(= 기존 스택에서 쓰고 있던 컨트롤러)가 “새로운/다른 버전 스키마”를 기대/해석하게 됨

→ 이때 manifest validation, rule 파싱, selector 해석 등에서 **예전 스택이 예상 못 한 필드나 형식을 보게 되면서 에러**가 날 수 있어.

예를 들면:
- 기존 Rule이 새 스키마 기준으로는 invalid 해석돼서 Prometheus 웹 UI에 “some rules are invalid” 같은 에러
- 기존 스택이 보던 ServiceMonitor/PodMonitor의 label selector 동작이 바뀌거나, 의도치 않은 리소스를 같이 보기 시작함

### 2-2. Node Exporter / Kube-State-Metrics 같은 “클러스터 전체용” DaemonSet/Deployment 중복

`kube-prometheus-stack` 안에는:
- prometheus-node-exporter (DaemonSet)
- kube-state-metrics (Deployment)
- alertmanager, operator 등…

이런 것들이 들어있는데, 두 개의 스택이 동시에 깔리면:
1. node-exporter 두 세트가 같은 노드에 뜨려고 함
  - 보통 hostNetwork + 동일 포트(9100 등)를 쓰기 때문에 두 번째 DaemonSet이 스케줄링 실패 / CrashLoop가 날 수 있음
  - 그 과정에서 기존 Service/Endpoint 구성이 꼬일 수 있음 (어느 exporter를 바라보는지 애매해짐)
1. 기존 Prometheus가 새 node-exporter / kube-state-metrics 를 잘못 보게 됨
  - label/selector가 겹쳐버리면
  - “기존 Prometheus가 새 스택의 metrics 엔드포인트를 일부 보거나, 반대로 새 Prometheus가 기존 스택용 것들을 보거나” 섞여서,
  - 메트릭 구성/대시보드가 갑자기 예상과 다르게 바뀔 수 있음

이건 “프로메테우스 프로세스가 죽는다” 수준이 아니라,

**보이는 타겟/메트릭이 이상하게 꼬이는 계층의 문제**가 많이 생겨.

### 2-3. PrometheusRule / ServiceMonitor 들이 같은 라벨을 공유하면서 충돌

새 스택은 `release: prometheus` 라벨을 쓰고,

기존 스택은 `release: prometheus-stack` 일 거라고 나는 가정했었는데,
- 튜토리얼 install.sh 가 가져온 chart 버전이 달라서
- CRD의 기본 .spec.ruleSelector , .spec.podMonitorSelector 설정이 바뀌었거나
- helm values 의 default 값이 변경되면서

**기존 Prometheus가 봐야 할 리소스를 안 보거나, 반대로 Ray 쪽 것까지 보고 rule 충돌이 발생**할 수 있어.

예를 들어,
- 어떤 PrometheusRule 안에 syntax가 살짝 안 맞는 rule이 있는데,
- 예전엔 안 읽던 rule을 새 selector 로 인해 읽게 됨
- Prometheus는 “한 개 rule이라도 invalid면 전체를 실패로 보고 경고”를 띄움

→ 결과적으로 “기존 프로메테우스 UI에서 에러 표시”가 보일 수 있음.

---

## 3. “괜찮다”라고 말했던 포인트와, 실제로는 왜 리스크가 있었는지

내가 “설치해도 괜찮다”고 했던 건 **Helm release 관점**에서:
- 네임스페이스 다름
- release 이름 다름
- chart 설치는 대부분 “자기 네임스페이스” 안에서만 돌아감

이라는 기준이었는데,

**kube-prometheus-stack 만큼은 예외에 가까운 차트**였던 거지:
- CRD
- node-exporter (host network)
- cluster-wide metrics 에코시스템 전체를 끌고 들어오는 차트라
- “완전 독립된 두 인스턴스”로 보기엔 리스크가 큼

→ 이걸 내가 충분히 강조해서 “이건 다른 차트랑 달리, 두 번 설치는 싸함”이라고 이야기했어야 하는데, 그 부분이 부족했음. 그래서 네가 “너가 괜찮다해서…”라고 느낀 거고, 그건 내 잘못 맞아.

---

## 4. 지금 상황 복구 방향은 잘 잡았어

네가:

> “그 설치한거 지우자”

라고 했을 때 내가 제안한 흐름은:
1. helm uninstall prometheus -n prometheus-system
1. prometheus-system 안 PodMonitor / ServiceMonitor / PrometheusRule / PVC 정리
1. 필요하면 prometheus-system 네임스페이스 자체 삭제

이렇게 하면 **새로 깐 스택은 깨끗하게 제거**됐고,

지금 문제가 남아 있다면:
- CRD 스키마가 변경된 영향
- 기존 Prometheus/Operator가 그 상태에서 동작하면서 뭔가 rule/target 에러를 내는 상태

일 가능성이 큼.

만약 지금도 기존 Grafana/Prometheus가 이상하다면:
- Prometheus 웹 UI → /status / /config / /rules / /targets 에 나오는 경고/에러 메시지 보고
- 어떤 Rule/Target 에서 깨지는지 확인해서
- 그 Rule/ServiceMonitor/PodMonitor 만 부분적으로 정리

이런 식으로 하나씩 풀어줄 수 있어.

(에러 메시지/스크린샷 같은 거 알려주면, 내가 “어느 PrometheusRule / ServiceMonitor 때문인지”까지 같이 잡아줄게.)

---

## 5. 앞으로는 어떻게 하는 게 제일 안전하냐

요건 솔직히 내가 아까 설명한 전략을 다시 정리하는 거야:
- 운영/공용 모니터링 스택이 이미 있다면 → 웬만하면 kube-prometheus-stack 를 또 설치하지 말고 → **기존 스택에 PodMonitor/ServiceMonitor/PrometheusRule만 추가**해서 Ray metrics 붙이는 게 정석
- “완전 격리된 실험 클러스터(kind 같은 거)”라면 → 튜토리얼대로 새 Prometheus stack 설치 OK

근데 너는:
- 이미 monitoring 에 잘 돌고 있는 공용 스택 있음
- ray-system 는 Ray 실험용 네임스지만, 클러스터는 어쨌든 같은 클러스터

이라서, **원래는 “기존 Prometheus에 Ray 자원만 붙이는 방향”을 탔어야 더 안전한 상황**이긴 했어.

---

## 6. 그래서 한 줄로 요약하면
- 내가 “release/namespace만 다르면 괜찮다”고 단정적으로 말한 건 👉 **kube-prometheus-stack이 가진 클러스터 전역 영향(특히 CRD/DaemonSet)까지 충분히 고려하지 않은 실수**였고
- 실제로는:
  - CRD 재설치
  - node-exporter 중복
  - rule/selector 충돌 이런 쪽에서 기존 스택이 영향을 받았을 가능성이 크다

지금 상태가 여전히 깨져 있으면,

**Prometheus UI 에러 메시지 /**`**kubectl get prometheus -A -o yaml**`**일부 /**`**kubectl get pod -n monitoring**`**상태** 같은 것들 보여주면

“지금 어디가 꼬였는지”부터 같이 하나씩 풀어볼게.

일단은… 진짜 미안, 내가 조금 더 보수적으로 말했어야 했던 부분 맞아 🥲

지금부터는 **이미 설치된 스택 안 건드리고, 기존 Prometheus에 Ray만 예쁘게 붙이는 쪽**으로 다시 플랜 잡아서 정리해보자.

```powershell
$ ./install/prometheus/install.sh --auto-load-dashboard true
+ set -e
+ helm repo add prometheus-community https://prometheus-community.github.io/helm-charts
"prometheus-community" already exists with the same configuration, skipping
+ helm repo update
Hang tight while we grab the latest from your chart repositories...
...Successfully got an update from the "gpu-helm-charts" chart repository
...Successfully got an update from the "nvidia" chart repository
...Successfully got an update from the "jaegertracing" chart repository
...Successfully got an update from the "grafana" chart repository
...Successfully got an update from the "istio" chart repository
...Successfully got an update from the "bitnami" chart repository
...Successfully got an update from the "traefik" chart repository
...Successfully got an update from the "prometheus-community" chart repository
...Successfully got an update from the "kuberay" chart repository
...Successfully got an update from the "open-telemetry" chart repository
Update Complete. ⎈Happy Helming!⎈
+++ dirname ./install/prometheus/install.sh
++ cd ./install/prometheus
++ pwd
# ... verbose output omitted ...
deployment.apps/prometheus-kube-state-metrics         1/1     1            1           53s

NAME                                                            DESIRED   CURRENT   READY   AGE
replicaset.apps/prometheus-grafana-5bb9cb94f                    1         1         1       53s
replicaset.apps/prometheus-kube-prometheus-operator-cfd97b9bf   1         1         1       53s
replicaset.apps/prometheus-kube-state-metrics-688656ddbd        1         1         1       53s

NAME                                                                    READY   AGE
statefulset.apps/alertmanager-prometheus-kube-prometheus-alertmanager   0/1     53s
statefulset.apps/prometheus-prometheus-kube-prometheus-prometheus       0/1     53s
```

KubeRay는 다음을 수행하는 [install.sh 스크립트](https://github.com/wendyunji/kuberay/blob/master/install/prometheus/install.sh)를 제공합니다:
- namespace prometheus-system 에 kube-prometheus-stack v48.2.1 chart 및 관련 CR(PodMonitor, PrometheusRule 등)을 자동 설치
- -auto-load-dashboard true 를 설정한 경우 Ray Dashboard용 Grafana JSON 파일을 자동으로 가져옴 (만약 설정하지 않았다면 Step 12에서 수동으로 가져오는 방법을 제공합니다)

우리는 kube-prometheus-stack chart의 기본 values.yaml 을 일부 수정하여 Grafana panel이 Ray Dashboard에 embed되도록 허용하고 있습니다. 자세한 사항은 [overrides.yaml](https://github.com/wendyunji/kuberay/blob/master/install/prometheus/overrides.yaml) 참고.

```
grafana:
  grafana.ini:
    security:
      allow_embedding: true
    auth.anonymous:
      enabled: true
      org_role: Viewer
```

## **Step 3: KubeRay operator 설치**

---

Helm 저장소를 사용해 최신 안정 버전의 KubeRay operator를 설치하세요.

Helm으로 설치 시, KubeRay operator의 ServiceMonitor를 생성하도록 `metrics.serviceMonitor.enabled=true` 를 반드시 설정합니다.

Prometheus가 이를 찾을 수 있도록 `metrics.serviceMonitor.selector.release=prometheus` 도 설정합니다.

```
helm install kuberay-operator kuberay/kuberay-operator --version 1.5.0 \
  --set metrics.serviceMonitor.enabled=true \
  --set metrics.serviceMonitor.selector.release=prometheus
```

ServiceMonitor 생성 확인:

```
kubectl get servicemonitor
# NAME               AGE
# kuberay-operator   11s
```

### **→ 이미 설치되어있는 operator의 설정을 upgrade로 바꿔주기! run**

```powershell
$ helm upgrade kuberay-operator kuberay/kuberay-operator \
>   -n ray-system \
>   --version 1.5.0 \
>   --set metrics.serviceMonitor.enabled=true \
>   --set metrics.serviceMonitor.selector.release=prometheus

Release "kuberay-operator" has been upgraded. Happy Helming!
NAME: kuberay-operator
LAST DEPLOYED: Thu Nov 27 13:41:41 2025
NAMESPACE: ray-system
STATUS: deployed
REVISION: 2
TEST SUITE: None

$ kubectl get pods -n ray-system | grep kuberay
kuberay-operator-7fcc5dc5d7-cpcgj             1/1     Running   0              28s
raycluster-kuberay-head-7b6ft                 1/1     Running   0              7d4h
raycluster-kuberay-workergroup-worker-6lwzg   1/1     Running   3 (7d4h ago)   8d
$ kubectl get servicemonitor -A | grep kuberay
ray-system               kuberay-operator                                     48s
```

## **Step 4: RayCluster 설치**

---

```
# path: ray-operator/config/samples/
kubectl apply -f ray-cluster.embed-grafana.yaml
```

Service 생성 확인 (metrics endpoint는 port 8080):

```
kubectl get service -l ray.io/cluster=raycluster-embed-grafana
```

예시 출력:

```
NAME                                TYPE        CLUSTER-IP      EXTERNAL-IP   PORT(S)                                                    AGE
raycluster-embed-grafana-head-svc   ClusterIP   None            <none>        44217/TCP,10001/TCP,44227/TCP,8265/TCP,6379/TCP,8080/TCP   13m
```

모든 Ray Pod 준비될 때까지 대기:

```
kubectl wait pods -l ray.io/cluster=raycluster-embed-grafana --timeout 2m --for condition=Ready
```

Prometheus metrics endpoint 포트 포워딩:

```
kubectl port-forward service/raycluster-embed-grafana-head-svc metrics
```

다른 터미널에서 확인:

```
curl localhost:8080
```

예시 출력 (Prometheus metrics 형식):

```
# HELP ray_spill_manager_request_total Number of {spill, restore} requests.
# TYPE ray_spill_manager_request_total gauge
ray_spill_manager_request_total{Component="raylet", NodeAddress="<node-ip>", ...} 0
```

KubeRay는 기본적으로 port 8080을 통해 내장 exporter로 Prometheus metrics endpoint를 제공합니다. 따라서 외부 exporter를 설치할 필요가 없습니다.

환경 변수를 설정하기 위해 **ray-cluster.embed-grafana.yaml** 에 세 개의 필수 env 변수가 포함되어 있습니다:

```
env:
  - name: RAY_GRAFANA_IFRAME_HOST
    value: http://127.0.0.1:3000
  - name: RAY_GRAFANA_HOST
    value: http://prometheus-grafana.prometheus-system.svc:80
  - name: RAY_PROMETHEUS_HOST
    value: http://prometheus-kube-prometheus-prometheus.prometheus-system.svc:9090
```

Grafana는 head Pod 내부에 배포되지 않기 때문에:
- RAY_GRAFANA_HOST = head Pod에서 backend로 요청 보낼 때 사용
- RAY_GRAFANA_IFRAME_HOST = 브라우저에서 iframe 패널 불러올 때 사용

Prometheus와 Grafana를 로컬에서 port-forward 하므로, `RAY_GRAFANA_IFRAME_HOST=http://127.0.0.1:3000` 을 설정합니다.

`http://` 접두사는 필수입니다.

## **Step 5: PodMonitor를 사용한 Head Node metrics 수집**

---

RayService는 head Pod에 대해 두 개의 Service를 생성하는데, 이 때문에 잘못 설정하면 metrics가 중복 수집될 수 있습니다.

따라서 head Pod는 PodMonitor를 사용하는 것이 권장됩니다.

예시 PodMonitor:

```yaml
apiVersion: monitoring.coreos.com/v1
kind: PodMonitor
metadata:
  labels:
    release: prometheus
  name: ray-head-monitor
  namespace: prometheus-system
spec:
  jobLabel: ray-head
  namespaceSelector:
    matchNames:
      - default
  selector:
    matchLabels:
      ray.io/node-type: head
  podMetricsEndpoints:
    - port: metrics
      relabelings:
        - action: replace
          sourceLabels: [__meta_kubernetes_pod_label_ray_io_cluster]
          targetLabel: ray_io_cluster
    - port: as-metrics
      relabelings:
        - action: replace
          sourceLabels: [__meta_kubernetes_pod_label_ray_io_cluster]
          targetLabel: ray_io_cluster
    - port: dash-metrics
      relabelings:
        - action: replace
          sourceLabels: [__meta_kubernetes_pod_label_ray_io_cluster]
          targetLabel: ray_io_cluster
```

`install.sh` 가 위 PodMonitor(podMonitor.yaml)를 자동 생성하므로 따로 만들 필요는 없습니다.

relabelings 는 scraped metrics에 `ray_io_cluster` 라는 label을 추가합니다.

여러 RayCluster가 있을 때 클러스터별 metric 필터링이 가능해집니다.

## **Step 6: Worker Node metrics 수집 (PodMonitor)**

---

worker Pod 또한 PodMonitor를 사용합니다.

worker Pod들은 독립적으로 존재하며 replicaSet이 아니므로 Service로 묶는 방식은 적절하지 않습니다.

예시 PodMonitor:

```yaml
apiVersion: monitoring.coreos.com/v1
kind: PodMonitor
metadata:
  name: ray-workers-monitor
  namespace: prometheus-system
  labels:
    release: prometheus
spec:
  jobLabel: ray-workers
  namespaceSelector:
    matchNames: [default]
  selector:
    matchLabels:
      ray.io/node-type: worker
  podMetricsEndpoints:
  - port: metrics
    relabelings:
    - sourceLabels: [__meta_kubernetes_pod_label_ray_io_cluster]
      targetLabel: ray_io_cluster
```

## **Step 7: ServiceMonitor를 통한 KubeRay metrics 수집**

---

KubeRay operator는 RayCluster, RayService, RayJob 에 대한 metrics를 제공합니다.

Prometheus는 namespaceSelector 와 selector를 이용하여 Service를 탐색합니다.

Service 확인:

```
kubectl get service -n default -l app.kubernetes.io/name=kuberay-operator
```

## **Step 8: Recording Rule로 custom metrics 생성**

---

Recording Rule은 자주 사용되거나 계산 비용이 큰 PromQL 결과를 미리 계산해 custom metric으로 저장합니다.

예시:

```yaml
apiVersion: monitoring.coreos.com/v1
kind: PrometheusRule
metadata:
  name: ray-cluster-gcs-rules
  namespace: prometheus-system
  labels:
    release: prometheus
spec:
  groups:
  - name: ray-cluster-main-staging-gcs.rules
    interval: 30s
    rules:
    - record: ray_gcs_availability_30d
      expr: |
        (
          100 * (
            sum(rate(ray_gcs_update_resource_usage_time_bucket{container="ray-head", le="20.0"}[30d]))
            /
            sum(rate(ray_gcs_update_resource_usage_time_count{container="ray-head"}[30d]))
          )
        )
```

위 recording rule은 prometheusRules.yaml 에 정의되어 있으며, install.sh 가 자동 생성합니다.

## **Step 9: Alerting Rule 정의 (선택)**

---

Alerting Rule은 alert 조건을 정의하고, 조건 충족 시 Alertmanager로 전달됩니다.

예시:

```yaml
apiVersion: monitoring.coreos.com/v1
kind: PrometheusRule
metadata:
  name: ray-cluster-gcs-rules
  namespace: prometheus-system
  labels:
    release: prometheus
spec:
  groups:
  - name: ray-cluster-main-staging-gcs.rules
    interval: 30s
    rules:
    - alert: MissingMetricRayGlobalControlStore
      annotations:
        description: Ray GCS is not emitting any metrics for Resource Update requests
        summary: Ray GCS is not emitting metrics anymore
      expr: |
        ( absent(ray_gcs_update_resource_usage_time_bucket) == 1 )
      for: 5m
      labels:
        severity: critical
```

이 alerting rule도 prometheusRules.yaml 에 포함되어 있으며 install.sh 가 생성합니다.

## **Step 10: Prometheus Web UI 접속**

---

포트 포워딩:

```
kubectl port-forward -n prometheus-system service/prometheus-kube-prometheus-prometheus http-web
```

브라우저에서 접근:
- http://127.0.0.1:9090/targets
  - podMonitor/prometheus-system/ray-workers-monitor/0
  - serviceMonitor/prometheus-system/ray-head-monitor/0
- http://127.0.0.1:9090/graph
  - System Metrics
  - Application Metrics
  - Recording Rule 기반 Custom Metrics (ray_gcs_availability_30d)
- http://127.0.0.1:9090/alerts
  - Alerting Rule 확인

## **Step 11: Grafana 접속**

---

포트 포워딩:

```
kubectl port-forward -n prometheus-system service/prometheus-grafana 3000:http-web
```

Grafana 로그인 페이지: [http://127.0.0.1:3000/login](http://127.0.0.1:3000/login)

기본 계정:
- username: admin
- password: <retrieve-from-kubernetes-secret>

## **Step 12: Grafana Dashboard 수동 Import (선택)**

---

자동 로드되지 않은 경우 수동 Import:
1. 왼쪽 “Dashboards” 클릭
1. “New” → “Import”
1. “Upload JSON file”
1. JSON 선택

케이스:
- Ray 2.41.0 사용 시: GitHub 의 xxx_grafana_dashboard.json 파일 사용
- 그 외: head Pod 의 /tmp/ray/session_latest/metrics/grafana/dashboards/ 사용 `kubectl cp $(kubectl get pods --selector ray.io/node-type=head,ray.io/cluster=raycluster-embed-grafana -o jsonpath={..metadata.name}):/tmp/ray/session_latest/metrics/grafana/dashboards/ /tmp/`

## **Step 13: 여러 RayCluster CR별로 metrics 보기**

---

Grafana 대시보드는 기본적으로 Cluster 변수로 클러스터별 metrics 필터링이 가능합니다.

예시:
- raycluster-embed-grafana
- raycluster-embed-grafana-2

클러스터별 metric 화면을 구분해서 볼 수 있습니다.

## **Step 14: KubeRay operator Dashboard 보기**

---

KubeRay operator dashboard를 가져오면 RayCluster, RayJob, RayService 의 controller runtime metrics 를 선택적으로 볼 수 있습니다.

## **Step 15: Ray Dashboard에 Grafana panel embed하기 (선택)**

---

```
kubectl port-forward service/raycluster-embed-grafana-head-svc dashboard
```

브라우저에서 방문:

[http://127.0.0.1:8265/#/metrics](http://127.0.0.1:8265/)

Grafana panel이 embed된 Ray Dashboard가 표시됩니다.

---

## 운영 관점에서 남긴 결론

클러스터 공용 Prometheus 스택은 하나만 운영하고, Ray 워크로드별 PodMonitor와 ServiceMonitor를 분리한다. 설정값을 외우기보다 제어면과 실행면, 워크로드 수명주기, 관측 가능한 성공 조건을 분리해 보는 것이 핵심입니다.

*실환경을 식별할 수 있는 호스트명·IP·내부 경로는 공개용 예시로 일반화했습니다.*
