> **TL;DR**  
> Ray CLI가 사용하는 Dashboard·Jobs API 경로만 명시적으로 노출하고, 서비스 선택과 포트 매핑을 분리해 관리한다.

이 글은 Ray와 KubeRay를 실제 ML 플랫폼 관점에서 이해하기 위해 정리한 학습 기록입니다. 개념 소개에 그치지 않고, 어떤 운영 문제를 해결하는지와 확인해야 할 지점을 중심으로 구성했습니다.

**다루는 주제**: Istio · Ray CLI · VirtualService · Job API

---

## Ray CLI 외부 접속 설정

> 외부 PC에서 Ray CLI를 사용해 Kubernetes 클러스터의 RayCluster에 job을 제출하는 방법입니다.

## 문제 정의

Ray Dashboard 웹 UI는 서브패스(`/ray`)에서 정상 작동하지만, Ray CLI는 서브패스를 지원하지 않습니다.

```bash
# ❌ 작동하지 않음
ray job submit --address <https://example.com/ray> --working-dir . -- python script.py
```

**원인**: Ray CLI SDK가 URL을 파싱할 때 경로 부분(`/ray`)을 잃어버리거나 잘못 처리함.

## 해결 방법

별도의 API 전용 경로(`/ray-api`)를 추가하고, Nginx 프록시를 거치지 않고 Ray Dashboard로 직접 라우팅합니다.

### 아키텍처

```
┌─────────────────────────────────────────────────────────────┐
│ 웹 브라우저: <https://example.com/ray>                        │
│   ↓                                                          │
│ Istio Gateway → Nginx Proxy → Ray Dashboard                │
│   (HTML 수정으로 서브패스 지원)                              │
└─────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────┐
│ Ray CLI: <https://example.com/ray-api>                        │
│   ↓                                                          │
│ Istio Gateway → Ray Dashboard (직접)                        │
│   (API만 사용, HTML 수정 불필요)                             │
└─────────────────────────────────────────────────────────────┘
```

### VirtualService 설정

```yaml
http:
  # Ray CLI/SDK API endpoint (직접 연결)
  - match:
    - uri:
        prefix: /ray-api/
    rewrite:
      uri: /
    route:
    - destination:
        host: raycluster-kuberay-head-svc.namespace.svc.cluster.local
        port:
          number: 8265
  - match:
    - uri:
        exact: /ray-api
    rewrite:
      uri: /
    route:
    - destination:
        host: raycluster-kuberay-head-svc.namespace.svc.cluster.local
        port:
          number: 8265

  # Ray Dashboard Web UI (Nginx 프록시 경유)
  - match:
    - uri:
        prefix: /ray/
    rewrite:
      uri: /
    route:
    - destination:
        host: ray-dashboard-nginx.namespace.svc.cluster.local
        port:
          number: 80
```

---

## 운영 관점에서 남긴 결론

Ray CLI가 사용하는 Dashboard·Jobs API 경로만 명시적으로 노출하고, 서비스 선택과 포트 매핑을 분리해 관리한다. 설정값을 외우기보다 제어면과 실행면, 워크로드 수명주기, 관측 가능한 성공 조건을 분리해 보는 것이 핵심입니다.

*실환경을 식별할 수 있는 호스트명·IP·내부 경로는 공개용 예시로 일반화했습니다.*
