> **TL;DR**  
> Ray 자체와 GPU collective 통신을 분리해서 보면 디버깅 경로가 선명해진다. 호스트, Pod, NCCL 순서로 RDMA 경로를 검증한다.

이 글은 Ray와 KubeRay를 실제 ML 플랫폼 관점에서 이해하기 위해 정리한 학습 기록입니다. 개념 소개에 그치지 않고, 어떤 운영 문제를 해결하는지와 확인해야 할 지점을 중심으로 구성했습니다.

**다루는 주제**: KubeRay · NCCL · InfiniBand · RDMA

---

지금 상황을 정리하면:
- 쿠버네티스 클러스터 + KubeRay + Ray 2.46.0(-gpu)
- InfiniBand 장비는 있고 , worker 그룹(gpu-*)에 /dev/infiniband + 호스트 lib를 이미 마운트 해둔 상태
- DeepSpeed + Ray Train 예제 코드 도 준비 완료
- 목표: 멀티 노드에서 NCCL/DeepSpeed가 GPUDirect RDMA(InfiniBand)를 타게 하기

여기서 중요 포인트 하나:

> RDMA를 Ray 내부(텐서 전송용)에서 직접 쓰는 기능(RDT)은 **2.50.0에서 알파 릴리즈**됨.([GitHub](https://github.com/ray-project/ray/releases))
>
> 하지만 **DeepSpeed/PyTorch의 NCCL 통신은 Ray 버전 상관없이 RDMA를 쓸 수 있음**.
>
> 지금은 “Ray는 오케스트레이션, 실제 GPU↔GPU 통신은 NCCL+IB” 구조를 만드는 걸 목표로 잡는 게 현실적이야.

그래서 아래 순서는 “NCCL이 IB RDMA를 제대로 타게 만드는 체크리스트”라고 보면 돼.

## 1단계. 노드(호스트) 레벨 선행 조건 확인

---

### 1-1. IB 디바이스/드라이버 확인 (노드 쉘에서)

컨트롤 플레인 말고 **GPU 노드**(예: `gpu-node-a`)에 ssh 들어가서:

```bash
# IB 디바이스 목록
ibv_devices

# 디바이스 상세
ibv_devinfo | head

# 디바이스 파일 있는지
ls -l /dev/infiniband
```

기대하는 것:
- ibv_devices 에 mlx5_0 같은 HCA 이름 보임
- /dev/infiniband 아래에 uverbs0 , issm0 , rdma_cm 등 존재

없으면 아직 **IB 드라이버/OFED**가 제대로 안 올라온 상태.

### run, h100-13

```python
user@gpu-node-a:~$ ibv_devices
    device          	   node GUID
    ------          	----------------
    mlx5_0          	e8ebd30300cd32b0
    mlx5_1          	e8ebd30300cd32b1
    mlx5_2          	58a2e103002a0b78
    mlx5_3          	e8ebd30300cd32c8
    mlx5_4          	e8ebd30300cd32c9
    mlx5_5          	58a2e1030029b1f8
    mlx5_6          	58a2e1030029bad0

user@gpu-node-a:~$ ibv_devinfo | head
hca_id:	mlx5_0
	transport:			InfiniBand (0)
	fw_ver:				20.41.1000
	node_guid:			e8eb:d303:00cd:32b0
	sys_image_guid:			e8eb:d303:00cd:32b0
	vendor_id:			0x02c9
	vendor_part_id:			4123
	hw_ver:				0x0
	board_id:			MT_0000000225
	phys_port_cnt:			1

user@gpu-node-a:~$ ls -l /dev/infiniband
total 0
crw------- 1 root root 231,  64 Oct  3 16:22 issm0
crw------- 1 root root 231,  65 Oct  3 16:22 issm1
crw------- 1 root root 231,  66 Oct  3 16:22 issm2
crw------- 1 root root 231,  67 Oct  3 16:22 issm3
crw------- 1 root root 231,  68 Oct  3 16:22 issm4
crw------- 1 root root 231,  69 Oct  3 16:22 issm5
crw------- 1 root root 231,  70 Oct  3 16:22 issm6
crw-rw-rw- 1 root root  10, 122 Oct  3 16:22 rdma_cm
crw------- 1 root root 231,   0 Oct  3 16:22 umad0
crw------- 1 root root 231,   1 Oct  3 16:22 umad1
crw------- 1 root root 231,   2 Oct  3 16:22 umad2
crw------- 1 root root 231,   3 Oct  3 16:22 umad3
crw------- 1 root root 231,   4 Oct  3 16:22 umad4
crw------- 1 root root 231,   5 Oct  3 16:22 umad5
crw------- 1 root root 231,   6 Oct  3 16:22 umad6
crw-rw-rw- 1 root root 231, 192 Oct  3 16:22 uverbs0
crw-rw-rw- 1 root root 231, 193 Oct  3 16:22 uverbs1
crw-rw-rw- 1 root root 231, 194 Oct  3 16:22 uverbs2
crw-rw-rw- 1 root root 231, 195 Oct  3 16:22 uverbs3
crw-rw-rw- 1 root root 231, 196 Oct  3 16:22 uverbs4
crw-rw-rw- 1 root root 231, 197 Oct  3 16:22 uverbs5
crw-rw-rw- 1 root root 231, 198 Oct  3 16:22 uverbs6
```

### run, h100-14

```python
user@gpu-node-b:~$ ibv_devices
    device          	   node GUID
    ------          	----------------
    mlx5_0          	e8ebd30300086e4e
    mlx5_1          	e8ebd30300086e4f

user@gpu-node-b:~$ ibv_devinfo | head
hca_id:	mlx5_0
	transport:			InfiniBand (0)
	fw_ver:				20.41.1000
	node_guid:			e8eb:d303:0008:6e4e
	sys_image_guid:			e8eb:d303:0008:6e4e
	vendor_id:			0x02c9
	vendor_part_id:			4123
	hw_ver:				0x0
	board_id:			MT_0000000225
	phys_port_cnt:			1

user@gpu-node-b:~$ ls -l /dev/infiniband
total 0
crw------- 1 root root 231,  64 Dec  6 11:26 issm0
crw------- 1 root root 231,  65 Dec  6 11:26 issm1
crw-rw-rw- 1 root root  10, 122 Dec  6 11:26 rdma_cm
crw------- 1 root root 231,   0 Dec  6 11:26 umad0
crw------- 1 root root 231,   1 Dec  6 11:26 umad1
crw-rw-rw- 1 root root 231, 192 Dec  6 11:26 uverbs0
crw-rw-rw- 1 root root 231, 193 Dec  6 11:26 uverbs1
```

### run

```python
user@gpu-node-a:~$ ip addr show ib0
9: ib0: <BROADCAST,MULTICAST,UP,LOWER_UP> mtu 2044 qdisc mq state UP group default qlen 256
    link/infiniband 00:00:09:3c:fe:80:00:00:00:00:00:00:e8:eb:d3:03:00:cd:32:b0 brd 00:ff:ff:ff:ff:12:40:1b:ff:ff:00:00:00:00:00:00:ff:ff:ff:ff
    altname ibp1s0f0
    inet <node-ip>/16 brd <node-ip> scope global ib0
       valid_lft forever preferred_lft forever
    inet6 fe80::eaeb:d303:cd:32b0/64 scope link
       valid_lft forever preferred_lft forever

user@gpu-node-b:~$ ip addr show ib0
9: ib0: <BROADCAST,MULTICAST,UP,LOWER_UP> mtu 2044 qdisc mq state UP group default qlen 256
    link/infiniband 00:00:06:ed:fe:80:00:00:00:00:00:00:e8:eb:d3:03:00:08:6e:4e brd 00:ff:ff:ff:ff:12:40:1b:ff:ff:00:00:00:00:00:00:ff:ff:ff:ff
    altname ibp194s0f0
    inet <node-ip>/16 brd <node-ip> scope global ib0
       valid_lft forever preferred_lft forever
    inet6 fe80::eaeb:d303:8:6e4e/64 scope link
       valid_lft forever preferred_lft forever
```

### 1-2. 노드 간 IB 레벨 통신

IB 인터페이스(`ib0` 등)에 IP가 붙어 있다면:

```bash
ip addr show ib0   # 혹은 eno_ib0 같은 이름

# 다른 GPU 노드의 IB IP로 ping
ping <다른-gpu-노드-ib-ip>
```

> “IB + IPoIB”가 살아 있는지 확인하는 단계야. 이게 되어야 상위 레이어에서 NCCL이 이 인터페이스를 골라 쓸 수 있음.

### run

```python
user@gpu-node-a:~$ ping -c 4 <node-ip>
PING <node-ip> (<node-ip>) 56(84) bytes of data.
64 bytes from <node-ip>: icmp_seq=1 ttl=64 time=0.340 ms
64 bytes from <node-ip>: icmp_seq=2 ttl=64 time=0.235 ms
64 bytes from <node-ip>: icmp_seq=3 ttl=64 time=0.218 ms
64 bytes from <node-ip>: icmp_seq=4 ttl=64 time=0.324 ms

--- <node-ip> ping statistics ---
4 packets transmitted, 4 received, 0% packet loss, time 3050ms
rtt min/avg/max/mdev = 0.218/0.279/0.340/0.053 ms

user@gpu-node-b:~$ ping -c 4 <node-ip>
PING <node-ip> (<node-ip>) 56(84) bytes of data.
64 bytes from <node-ip>: icmp_seq=1 ttl=64 time=0.833 ms
64 bytes from <node-ip>: icmp_seq=2 ttl=64 time=0.217 ms
64 bytes from <node-ip>: icmp_seq=3 ttl=64 time=0.229 ms
64 bytes from <node-ip>: icmp_seq=4 ttl=64 time=0.235 ms

--- <node-ip> ping statistics ---
4 packets transmitted, 4 received, 0% packet loss, time 3077ms
rtt min/avg/max/mdev = 0.217/0.378/0.833/0.262 ms
```

## 2단계. 쿠버네티스에서 RDMA/IB 노출 확인

---

K8s 쪽에서 RDMA를 쓰려면 보통:
- NVIDIA GPU Operator (GPU)
- NVIDIA Network Operator 또는 IB/ROCE용 CNI/플러그인 (RDMA)( NVIDIA Docs )

가 설치되어 있고, 노드에 `/dev/infiniband`가 파드에 노출 가능해야 해.

이미 Helm values에:

```yaml
volumes:
  - name: dev-infiniband
    hostPath:
      path: /dev/infiniband
      type: DirectoryOrCreate
  - name: host-ib-libs
    hostPath:
      path: /usr/lib/x86_64-linux-gnu
      type: Directory

volumeMounts:
  - name: dev-infiniband
    mountPath: /dev/infiniband
  - name: host-ib-libs
    mountPath: /host/lib/x86_64-linux-gnu
    readOnly: true

securityContext:
  capabilities:
    add:
      - IPC_LOCK
```

이렇게 잘 들어가 있어서 **파드 내부에서 /dev/infiniband + 호스트 IB 라이브러리 접근**은 이미 준비된 상태로 보임. (gpudirect/RDMA에서 `IPC_LOCK` 권한 필요하다는 문서가 많음([vLLM Forums](https://discuss.vllm.ai/t/deploying-multi-node-llm-with-infiband-roce/1344)))

여기까지 되었다고 치고, 이제 실제로 **NCCL이 IB를 타도록 환경변수/테스트**를 정리하자.

## 3단계. RayCluster Helm values 튜닝 (NCCL + IB 쪽)

---

지금 `gpuWorkerGroup`부터 `gpuXlargeGroup`까지는 아래 env가 공통:

```yaml
containerEnv:
  - name: NCCL_DEBUG
    value: "INFO"
  - name: NCCL_IB_DISABLE
    value: "0"
  - name: NCCL_NET_GDR_LEVEL
    value: "5"
  - name: LD_LIBRARY_PATH
    value: "/host/lib/x86_64-linux-gnu:$LD_LIBRARY_PATH"
```

여기에 **조금만 더 추가**해 주면 좋아:

```yaml
containerEnv:
  - name: NCCL_DEBUG
    value: "INFO"
  - name: NCCL_IB_DISABLE
    value: "0"
  # IB HCA 선택 (ibv_devices로 본 이름에 맞게 수정)
  - name: NCCL_IB_HCA
    value: "mlx5_0"
  # IB 인터페이스 이름 (ip addr로 확인: ib0 등)
  - name: NCCL_SOCKET_IFNAME
    value: "ib0"
  # GPUDirect RDMA 사용 레벨 (이미 있음)
  - name: NCCL_NET_GDR_LEVEL
    value: "5"
  # RDMA + fork 안전
  - name: RDMAV_FORK_SAFE
    value: "1"
  # (선택) NCCL 프로토콜 단순화 – 디버그/테스트에 도움
  - name: NCCL_PROTO
    value: "SIMPLE"
  - name: LD_LIBRARY_PATH
    value: "/host/lib/x86_64-linux-gnu:$LD_LIBRARY_PATH"
```

> vLLM + Ray를 IB 위에서 돌리는 가이드들에서도 필수로 넣으라고 되어 있음.([vLLM Forums](https://discuss.vllm.ai/t/deploying-multi-node-llm-with-infiband-roce/1344))

**Head Pod**에도 (GPU는 없어도) 같은 NCCL/IB env를 넣어두면,

나중에 head가 통신에 끼는 워크로드(NCCL 테스트, 간단한 torch.distributed init 등)에서도 헷갈리지 않아서 좋아.

## 4단계. NCCL + IB 스모크 테스트 (Ray/DeepSpeed 전에)

---

DeepSpeed/Ray Train 넣기 전에, **그냥 NCCL 테스트 파드**로 RDMA가 진짜 타는지부터 확인하는 걸 강력 추천.

### 4-1. gpu-worker과 동일한 스펙의 debug Pod 하나 만들기

예시 (대충 `nccl-test-pod.yaml` 같은 느낌):

```yaml
apiVersion: v1
kind: Pod
metadata:
  name: nccl-test
spec:
  restartPolicy: Never
  nodeSelector:
    # gpu-worker이 스케줄되는 노드 셀렉터가 있다면 맞춰주기
  containers:
    - name: nccl-test
      image: nvidia/cuda:12.2.0-devel-ubuntu22.04   # 또는 rayproject/ray:2.46.0-gpu 그대로 써도 됨
      command: ["/bin/bash", "-c"]
      args:
        - |
          nvidia-smi;
          ls -l /dev/infiniband || echo "no infiniband";
          ibv_devices || echo "no ibv_devices";
          # 여기서 nccl-tests 깔고 all_reduce_perf 같은 거 돌릴 수 있음
          sleep 3600
      resources:
        limits:
          nvidia.com/gpu: 1
      volumeMounts:
        - name: dev-infiniband
          mountPath: /dev/infiniband
        - name: host-ib-libs
          mountPath: /host/lib/x86_64-linux-gnu
          readOnly: true
      securityContext:
        capabilities:
          add:
            - IPC_LOCK
  volumes:
    - name: dev-infiniband
      hostPath:
        path: /dev/infiniband
        type: DirectoryOrCreate
    - name: host-ib-libs
      hostPath:
        path: /usr/lib/x86_64-linux-gnu
        type: Directory
```

이 파드 안에 들어가서:

```bash
kubectl exec -it nccl-test -- bash

# 파드 안에서
ls /dev/infiniband
ibv_devices
```

이 두 개만 잘 보이면 **IB 디바이스가 컨테이너에 잘 노출된 것**이고,

이후엔 `nccl-tests`(all_reduce_perf 등)를 두 노드에서 띄워서 **RDMA 성능**을 확인할 수 있어.

## 5단계. RayCluster + DeepSpeed Ray Train 조합에서 RDMA 타기

---

이제 네가 이미 가진 RayCluster + DeepSpeed 예제로 돌아오자.

### 5-1. RayCluster 쪽
- gpuWorkerGroup 에 위에서 말한 NCCL 환경변수들 추가
- replicas 최소 2개 이상으로 설정해 멀티 노드 실험:

```yaml
gpuWorkerGroup:
  replicas: 2
  minReplicas: 2
  maxReplicas: 7
  ...
```

RayCluster 올라온 뒤:

```bash
kubectl get pods -l ray.io/node-type=worker -o wide
```

을 보면 `gpu-worker` worker들이 **서로 다른 노드**에 분산되어 뜨도록 해주면 멀티노드 테스트에 좋아.

### 5-2. DeepSpeed + Ray Train 코드 쪽 체크

너 코드의 핵심 부분:

```python
trainer = TorchTrainer(
    train_func,
    train_loop_config=training_config,
    scaling_config=ScalingConfig(
        num_workers=5,
        use_gpu=True,
        resources_per_worker={"GPU": 1, "gpu-worker": 1}
    ),
    run_config=ray.train.RunConfig(storage_path="/mnt/shared/ray_results"),
)
```

여기에서 중요한 점:
- resources_per_worker={"GPU": 1, "gpu-worker": 1} → Ray scheduler가 `gpu-worker` 리소스가 1인 worker만 잡도록 해서, **지금 설정한 gpuWorkerGroup에서만 worker가 뜨도록 잘 묶어두었음** (이건 아주 좋음).
- DeepSpeed는 GPU 있으면 기본 backend로 NCCL 을 쓰기 때문에, 우리가 한 NCCL 환경변수 설정이 그대로 적용됨.([docs.ray.io](https://docs.ray.io/en/latest/train/deepspeed.html))

추가로 파이썬 코드 상에서 굳이 backend를 건드리지 않아도 되지만,

확실히 하고 싶으면 `deepspeed.initialize` 전에 한번 로깅 정도는 할 수 있음:

```python
import torch.distributed as dist

def train_func(config):
    ...
    if dist.is_available():
        print("torch.distributed backend nccl available?:", dist.is_nccl_available())
    ...
```

### 5-3. NCCL이 실제로 IB를 타는지 로그 확인

이미 env에 `NCCL_DEBUG=INFO`를 넣었으니,

**Ray job 로그**에서 이런 문자열을 찾아볼 수 있으면 성공:
- NET/IB : Dev mlx5_0 Port 1
- NCCL INFO NET/Socket : Using [ib0]
- PeerDirect RDMA 같은 단어들

이런 로그는 기본적으로 **NCCL이 TCP 대신 InfiniBand RDMA 경로를 쓰고 있다**는 강력한 신호야.([vLLM Forums](https://discuss.vllm.ai/t/deploying-multi-node-llm-with-infiband-roce/1344))

---

## 6단계. (선택) Ray Direct Transport(RDT)까지 쓰고 싶다면

지금 이미 **DeepSpeed + NCCL + RDMA** 조합은 위 단계까지만 해도 구현 가능해.

만약 “Ray 내부에서 텐서를 넘길 때도 RDMA를 쓰는 RDT”까지 해보고 싶으면:
1. Ray 버전을 2.50.0 이상 으로 올려야 하고,( GitHub )
1. 이미지도 rayproject/ray:2.52.1-gpu 계열 + UCX/NIXL 등이 들어간 커스텀 이미지로 가는 게 현실적 (CoreWeave, AKS, GKE 예제들이 이런 식으로 가이드함).( Anyscale )
1. 코드에서 RDT API ( tensor_transport 파라미터 및 ray.experimental.direct_transport 계열)를 써야 함.

이건 조금 더 “연구/실험” 느낌이니까,

**지금 단계에서는 일단 NCCL RDMA를 확실하게 잡는 것**부터 마무리하는 게 좋아 보여.

---

## 요약해서 “지금 당장 할 일”만 다시 정리하면
1. GPU 노드에서 ibv_devices , /dev/infiniband 확인
1. K8s에서 debug pod 띄워서 컨테이너 안에서도 /dev/infiniband , ibv_devices 확인
1. KubeRay Helm values의 gpuWorkerGroup (및 다른 gpu-* 그룹)에:
  - NCCL_IB_HCA , NCCL_SOCKET_IFNAME , RDMAV_FORK_SAFE 등 env 추가
1. RayCluster 재배포 후,
  - gpu-worker worker 2개 이상, 서로 다른 노드에 올라가게 구성
1. DeepSpeed 예제 job 실행 후,
  - Ray job 로그에서 NCCL INFO 가 InfiniBand/IB/PeerDirect 관련 메시지를 찍는지 확인

이 단계들 중에서 **어디까지 해봤는지**, 혹은 실행 중에 나온 `NCCL INFO` 로그 몇 줄만 복붙해주면

그 다음엔 로그를 기준으로 “아 이제 RDMA 제대로 타고 있다 / 여긴 더 고쳐야 한다” 같이 더 구체적으로 짚어볼게.

아! **RLIMIT_MEMLOCK (locked memory)**가 정확히 무엇이고 언제 사용되는지 설명드리겠습니다.

## Locked Memory란?

**Locked Memory**는 리눅스에서 **물리 메모리(RAM)에 고정되어 swap out되지 않는 메모리**를 말합니다.

일반적으로 리눅스는 메모리가 부족하면:
1. 사용 안 하는 메모리 페이지를 디스크(swap)로 이동 시킵니다
1. 나중에 필요하면 다시 RAM으로 읽어옵니다

하지만 **mlock() 시스템 콜**로 메모리를 "lock"하면:
- 그 메모리는 절대 swap out되지 않고 RAM에 계속 존재 합니다
- 하드웨어가 물리 주소 를 직접 접근할 수 있습니다

## InfiniBand RDMA가 Locked Memory를 쓰는 이유
- *RDMA (Remote Direct Memory Access)**는:
1. CPU를 거치지 않고 네트워크 카드(InfiniBand HCA)가 메모리에 직접 접근 합니다
1. 네트워크 카드는 물리 메모리 주소 만 알고 가상 메모리는 모릅니다
1. 만약 메모리가 swap out되면:
  - 네트워크 카드가 접근할 때 잘못된 메모리 를 읽게 됩니다
  - 데이터 손상 이나 시스템 크래시 가 발생합니다

따라서 RDMA를 사용하는 메모리는 **반드시 물리 RAM에 고정**되어야 합니다.

## InfiniBand가 얼마나 많은 Locked Memory를 필요로 하나?

RDMA는 다음과 같은 메모리를 lock해야 합니다:
1. 송수신 버퍼 : 네트워크 데이터가 오가는 버퍼
1. GPU 메모리와 통신하는 호스트 메모리 : GPUDirect RDMA 사용 시
1. NCCL 통신 버퍼 : 분산 학습에서 gradient 교환용

**H100 3개 GPU 학습**의 경우:
- 각 GPU당 수백 MB ~ 수 GB의 통신 버퍼 필요
- NCCL이 자동으로 필요한 만큼 mlock() 호출
- 64 KB는 턱없이 부족 합니다 (mlock() 첫 호출에서 바로 실패)

## 실제 에러 상황

```
ibv_create_cq failed with error Cannot allocate memory
```

이 에러는:
1. NCCL이 InfiniBand를 초기화하려고 함
1. ibv_create_cq() 함수가 내부적으로 mlock() 호출
1. 현재 프로세스의 locked memory가 64 KB 제한에 도달
1. mlock() 실패 → ENOMEM (Cannot allocate memory)
1. NCCL 초기화 실패 → 학습 실패

## 요약
- Locked Memory : 물리 RAM에 고정되어 swap out 안 되는 메모리
- 왜 필요한가 : RDMA 네트워크 카드가 물리 주소로 직접 메모리 접근
- 64 KB 문제 : InfiniBand 초기화만 해도 수 MB 필요한데 64 KB는 너무 작음
- 해결 : unlimited로 설정하거나 최소 수백 MB 이상 필요

그래서 containerd 설정에서 `Memlock = -1` (unlimited)로 바꿔야 InfiniBand RDMA가 작동합니다!
1. 문제 상황 : InfiniBand RDMA에서 64 KB memlock 제한으로 인한 ibv_create_cq 실패
1. 시도한 7가지 방법 :
  - Custom Docker 이미지 with Entrypoint Wrapper ❌
  - CRD Patch로 Container Command Override ❌
  - Bash Wrapper로 /bin/bash 교체 ❌
  - SYS_RESOURCE Capability 추가 ❌
  - hostNetwork 사용 ❌
  - shareProcessNamespace + initContainer ❌
  - KubeRay v1.1.0+ overwrite-container-cmd ❌
1. 실패 원인 : 컨테이너 프로세스가 시작된 후에는 hard limit을 올릴 수 없음 (Linux 커널 제약)
1. 유일한 해결책 : containerd 설정 수정 또는 RuntimeClass 사용
1. 현재 상태 : 모든 Kubernetes 네이티브 방법 시도 완료, 노드 레벨 설정 변경 필요

---

## 운영 관점에서 남긴 결론

Ray 자체와 GPU collective 통신을 분리해서 보면 디버깅 경로가 선명해진다. 호스트, Pod, NCCL 순서로 RDMA 경로를 검증한다. 설정값을 외우기보다 제어면과 실행면, 워크로드 수명주기, 관측 가능한 성공 조건을 분리해 보는 것이 핵심입니다.

*실환경을 식별할 수 있는 호스트명·IP·내부 경로는 공개용 예시로 일반화했습니다.*
