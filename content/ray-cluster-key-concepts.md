> **TL;DR**  
> Head는 제어면, Worker는 실행면이다. Autoscaler는 논리 리소스 요구를 Kubernetes Pod 확장으로 연결한다.

이 글은 Ray와 KubeRay를 실제 ML 플랫폼 관점에서 이해하기 위해 정리한 학습 기록입니다. 개념 소개에 그치지 않고, 어떤 운영 문제를 해결하는지와 확인해야 할 지점을 중심으로 구성했습니다.

**다루는 주제**: Ray Cluster · Head Node · Worker Node · Autoscaling

---

## 주요 개념(Key Concepts)

> 이 페이지는 Ray 클러스터에 대한 핵심 개념을 소개합니다.

## **Ray Cluster**

---

Ray 클러스터는 **하나의 Head 노드**와 **여러 개의 Worker 노드**로 구성됩니다.
- 각 노드는 분산 스케줄링과 메모리 관리를 돕는 Ray의 헬퍼 프로세스를 실행합니다.
- Head 노드는 여기에 더해 클러스터 관리용 프로세스들 (파란색 강조 영역)을 추가로 실행합니다.
- 클러스터 구성에 따라 워커 노드 수는 애플리케이션의 수요에 맞게 오토스케일링 될 수 있습니다.
- 오토스케일러는 Head 노드에서 동작합니다.
- Kubernetes 환경에서 Ray 노드는 각각 **Pod** 로 구현됩니다.
- 사용자는 Ray 클러스터에 잡(Job) 을 제출하거나 Head 노드에 접속해 ray.init() 으로 인터랙티브하게 사용 할 수 있습니다.

## **Head Node**

---

모든 Ray 클러스터에는 하나의 Head 노드가 있습니다.

Head 노드는 일반 Worker 노드와 거의 동일하지만, 다음과 같은 **클러스터 관리용 단일 프로세스**가 추가로 실행됩니다:
- Autoscaler
- GCS(Global Control Store)
- Ray Driver 프로세스(Ray Jobs 실행)

Head 노드 역시 Worker 노드처럼 태스크나 액터가 스케줄될 수 있지만, **대규모 클러스터에서는 보통 이것을 피하는 것이 좋습니다.**

## **Worker Node**

---

Worker 노드는 Head 노드의 관리 프로세스를 실행하지 않으며,

**Ray Task와 Actor를 실행****하는 데만 사용됩니다.**

또한 분산 스케줄링에 참여하고, Ray Object를 클러스터 메모리에 저장·배포하는 역할을 수행합니다.

## **Autoscaling**

---

Ray Autoscaler는 Head 노드에서 동작하는 프로세스입니다 (Kubernetes에서는 Head Pod의 사이드카 컨테이너로도 실행될 수 있습니다).

Ray 워크로드의 리소스 요구량이 현재 클러스터의 처리 능력을 초과하면, Autoscaler가 Worker 노드를 스케일 아웃합니다.

반대로 Worker 노드가 일정 시간 동안 유휴 상태라면, Autoscaler는 해당 노드를 제거합니다.

중요한 점은 Autoscaler는 **태스크/액터가 요청한 리소스 기반으로만 동작**하며,

애플리케이션 메트릭이나 실제 물리적 리소스 사용량에는 반응하지 않는다는 것입니다.

## **Ray Jobs**

---

Ray Job은 하나의 애플리케이션을 의미합니다.

즉, **동일한 스크립트에서 시작된****Ray Task, Object, Actor의 집합**입니다.

이 스크립트를 실행하는 워커는 **Driver**라고 불립니다.

Ray Job을 실행하는 방법은 두 가지입니다:
1. (권장) Ray Jobs API를 사용해 Job 제출
1. Head 노드에서 직접 드라이버 스크립트를 실행해 인터랙티브하게 작업

---

## 운영 관점에서 남긴 결론

Head는 제어면, Worker는 실행면이다. Autoscaler는 논리 리소스 요구를 Kubernetes Pod 확장으로 연결한다. 설정값을 외우기보다 제어면과 실행면, 워크로드 수명주기, 관측 가능한 성공 조건을 분리해 보는 것이 핵심입니다.

*실환경을 식별할 수 있는 호스트명·IP·내부 경로는 공개용 예시로 일반화했습니다.*
