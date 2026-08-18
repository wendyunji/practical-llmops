> **TL;DR**  
> 공용 실행 기반은 RayCluster, 끝이 있는 작업은 RayJob, 장기 서빙은 RayService로 나누면 책임과 장애 범위가 선명해진다.

이 글은 Ray와 KubeRay를 실제 ML 플랫폼 관점에서 이해하기 위해 정리한 학습 기록입니다. 개념 소개에 그치지 않고, 어떤 운영 문제를 해결하는지와 확인해야 할 지점을 중심으로 구성했습니다.

**다루는 주제**: RayCluster · RayJob · RayService · Architecture

---

## RayCluster / Ray Job / Ray Service

| 항목 | RayCluster | Ray Job | Ray Service (RayServe) |
| --- | --- | --- | --- |
| 목적 | Ray 런타임/클러스터 자체 | 1회성 배치 Job 실행 | 지속형 모델 Serving |
| 수명 | 장기 | 단기(끝나면 종료) | 장기(서비스 지속) |
| 필요 자원 | Pod(H/W) 생성 및 유지 | 기존 Cluster 리소스 사용 | 클러스터 + 자동 스케일링 |
| K8s CRD | `RayCluster` | `RayJob` | `RayService` |
| 대표 사용 | 분산 학습, 플랫폼 기반 | 훈련/전처리 | LLM/ML API 운영 |

## 1) RayCluster

**“Ray 런타임을 구성하는 인프라 자체(클러스터)”**

---

**무엇을 위한 것?**
- 워커/헤드 노드로 구성된 Ray 런타임 환경 자체를 만드는 것
- 그 위에 Ray Job, Ray Serve 등을 올릴 수 있는 기반 인프라 레이어

**언제 쓰나?**
- 장기 운영되는 Ray 기반 플랫폼
- 분산 학습(DDP, FSDP) 실행 클러스터
- Ray Serve 배포 기반
- 여러 Job을 지속적으로 실행해야 하는 환경

**Kubernetes에서는?**
- KubeRay CRD: RayCluster
- Operator가 Head/Worker Pod을 자동 생성
- GPU/CPU 노드 자동 스케일링 가능

**요약**

> “클러스터 자체”

## 2) Ray Job

**“Ray 클러스터에 제출하는 1회성 배치 작업”**

---

**무엇을 위한 것?**
- 스크립트 하나 실행
- 훈련/전처리/데이터 파이프라인 같이 시작하고 끝나는 Job

**특징**
- RayCluster가 있어야 실행 가능
- Job이 끝나면 서비스는 종료
- API나 CLI로 제출
- Pod을 새로 만드는 게 아니라, 기존 RayCluster의 리소스를 사용

**Kubernetes에서는?**
- RayJob CRD를 써서 “RayCluster 생성 + Job 실행 + 완료되면 삭제”까지 자동으로 가능

**언제 쓰나?**
- 훈련 작업
- 데이터 전처리
- 단건 inference pipeline
- GPU batch workload

**요약**

> “한 번 실행하고 끝나는 작업” = Ray Job.

## 3) Ray Service (Ray Serve)

**“지속적으로 트래픽을 받는 모델 서비스(Serve) 배포”**

---

정확한 명칭은 “Ray Serve”이고 Kubernetes CRD는 **RayService**.

**무엇을 위한 것?**
- 웹 트래픽을 처리하는 **지속형 Inference Serving**
- LLM, Embedding, RAG 서비스처럼 계속 살아있어야 하는 모델 서비스

**특징**
- Auto-scaling(트래픽 기반 스케일)
- Replica 관리
- 안정성 확보를 위한 RollingUpdate
- 서비스가 죽으면 다시 살림(Operator가 관리)
- Deployment + ServeConfig가 합쳐진 개념

**Kubernetes에서는?**
- RayService CRD
- 내부적으로 **RayCluster + ServeDeployment** 를 묶어서 운영
- 장애 발생 시 자동 롤백 / 재시작

**언제 쓰나?**
- 지속적인 LLM 서비스(vLLM 대비 multi-model, multi-deployment 가능)
- Recommendation API
- Streaming inference
- Traffic routing 실험(A/B 테스트)

**요약**

> 지속적이고 안정적인 모델 Serving 인프라 = Ray Service.

## Architecture in K8s

> 여러 개의 RayCluster를 동시에

## 1) 운영 패턴

---

### 패턴 A: 하나의 K8s 클러스터, 여러 RayCluster
- 예:
  - ray-train-cluster (분산 학습용 / GPU-heavy)
  - ray-batch-cluster (전처리/ETL용)
  - ray-serve-cluster (RayService용, 안정적인 Serving)
- 이 위에 Job도 보내고 Serve도 올림

**장점:** 리소스 격리, 장애 격리

**단점:** RayCluster가 많아지면 관리 복잡

### 패턴 B: 단일 RayCluster + Job + Serve 모두 얹기
- 시작 단계에서 흔히 이렇게 POC 운용
- 단일 RayCluster 생성 →
  - 배치 작업은 RayJob
  - 서비스는 RayService
  - 둘 다 같은 클러스터 리소스 사용

**장점:** 가장 단순한 구조

**단점:**
- Serve가 죽어서 cluster restart 되면 Job에 영향
- Job이 GPU 다 써버리면 Serve latency 올라감
- SLO가 필요한 서비스에는 부적합

## 2) 운영 시 주의사항

---

### RayCluster 간 리소스 경합

쿠버네티스 노드/GPUs는 공유되므로
- 각 RayCluster에 Resource Quota
- Pod Priority/Class
- Taint/Toleration
- NodeSelector 정책을 걸어주는 게 안전함.

특히 **Serving(RayService)은 안정성 필요 → 전용 GPU 노드 권장**

### RayJob이 대량으로 제출될 경우
- Worker auto-scaling이 매우 잦아질 수 있음
- 클러스터 오토스케일러가 감당 가능한지 체크
- Training workload는 GPU 몰아쓰기 가능하므로 Serving 영향 줄 수 있음

### RayService와 RayJob이 같은 Cluster에서 공존할 경우
- RayService는 항상 On
- Job은 burst workload → 둘의 workload 특성 차이 때문에 QoS 충돌이 가장 빈번함

대안:
- Serve는 별도 RayCluster로 분리
- Job/Training은 다른 RayCluster

## 3) 추후 계획

---

### 초반 POC 단계
- K8s 클러스터 하나
- RayCluster 1개
- Job/Service 다 얹어서 실험 → 복잡도↓, 실험 빠름

### 운영 전환할 때
- Serve 전용 RayCluster 따로 분리
- Training/Batch Job은 다른 RayCluster
- GPU Pool도 분리

→ 안정성↑, SLO 대응 가능

---

## 운영 관점에서 남긴 결론

공용 실행 기반은 RayCluster, 끝이 있는 작업은 RayJob, 장기 서빙은 RayService로 나누면 책임과 장애 범위가 선명해진다. 설정값을 외우기보다 제어면과 실행면, 워크로드 수명주기, 관측 가능한 성공 조건을 분리해 보는 것이 핵심입니다.

*실환경을 식별할 수 있는 호스트명·IP·내부 경로는 공개용 예시로 일반화했습니다.*
