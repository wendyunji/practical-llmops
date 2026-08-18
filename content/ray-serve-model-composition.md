> **TL;DR**  
> 모델 코드와 서빙 수명주기를 분리하고, Deployment composition으로 요약과 번역 단계를 독립적으로 운영한다.

이 글은 Ray와 KubeRay를 실제 ML 플랫폼 관점에서 이해하기 위해 정리한 학습 기록입니다. 개념 소개에 그치지 않고, 어떤 운영 문제를 해결하는지와 확인해야 할 지점을 중심으로 구성했습니다.

**다루는 주제**: Ray Serve · Model Serving · Composition · FastAPI

---

## **Ray Serve: 확장 가능하며 프로그래머블한 서빙 (Serving)**

> 응답 스트리밍, 동적 요청 배칭, 다중 노드/다중 GPU 서빙
>
> 모델 조합(model composition)
>
> 멀티 모델 서빙

## **Quickstart**

---

Ray Serve와 그 의존성을 설치한다:

```
pip install "ray[serve]"
```

간단한 “hello world” 애플리케이션을 정의하고 로컬에서 실행한 뒤 HTTP로 요청을 보내보자.

```python
import requests
from starlette.requests import Request
from typing import Dict

from ray import serve

# 1: Define a Ray Serve application.
@serve.deployment
class MyModelDeployment:
    def __init__(self, msg: str):
        # Initialize model state: could be very large neural net weights.
        self._msg = msg

    def __call__(self, request: Request) -> Dict:
        return {"result": self._msg}

app = MyModelDeployment.bind(msg="Hello world!")

# 2: Deploy the application locally.
serve.run(app, route_prefix="/")

# 3: Query the application and print the result.
print(requests.get("http://localhost:8000/").json())
# {'result': 'Hello world!'}
```

## **More examples**

---

### **Model composition**

Serve의 모델 조합 API를 사용해 여러 배포(deployment)를 하나의 애플리케이션으로 결합할 수 있다.

```python
import requests
import starlette
from typing import Dict
from ray import serve
from ray.serve.handle import DeploymentHandle

# 1. Define the models in our composition graph and an ingress that calls them.
@serve.deployment
class Adder:
    def __init__(self, increment: int):
        self.increment = increment

    def add(self, inp: int):
        return self.increment + inp

@serve.deployment
class Combiner:
    def average(self, *inputs) -> float:
        return sum(inputs) / len(inputs)

@serve.deployment
class Ingress:
    def __init__(
        self,
        adder1: DeploymentHandle,
        adder2: DeploymentHandle,
        combiner: DeploymentHandle,
    ):
        self._adder1 = adder1
        self._adder2 = adder2
        self._combiner = combiner

    async def __call__(self, request: starlette.requests.Request) -> Dict[str, float]:
        input_json = await request.json()
        final_result = await self._combiner.average.remote(
            self._adder1.add.remote(input_json["val"]),
            self._adder2.add.remote(input_json["val"]),
        )
        return {"result": final_result}

# 2. Build the application consisting of the models and ingress.
app = Ingress.bind(Adder.bind(increment=1), Adder.bind(increment=2), Combiner.bind())
serve.run(app)

# 3: Query the application and print the result.
print(requests.post("http://localhost:8000/", json={"val": 100.0}).json())
# {"result": 101.5}
```

### **FastAPI integration**

```powershell
import requests
from fastapi import FastAPI
from ray import serve

# 1: Define a FastAPI app and wrap it in a deployment with a route handler.
app = FastAPI()

@serve.deployment
@serve.ingress(app)
class FastAPIDeployment:
    # FastAPI will automatically parse the HTTP request for us.
    @app.get("/hello")
    def say_hello(self, name: str) -> str:
        return f"Hello {name}!"

# 2: Deploy the deployment.
serve.run(FastAPIDeployment.bind(), route_prefix="/")

# 3: Query the deployment and print the result.
print(requests.get("http://localhost:8000/hello", params={"name": "Theodore"}).json())
# "Hello Theodore!"
```

### **Hugging Face Transformers model**

```powershell
import requests
from starlette.requests import Request
from typing import Dict

from transformers import pipeline

from ray import serve

# 1: Wrap the pretrained sentiment analysis model in a Serve deployment.
@serve.deployment
class SentimentAnalysisDeployment:
    def __init__(self):
        self._model = pipeline("sentiment-analysis")

    def __call__(self, request: Request) -> Dict:
        return self._model(request.query_params["text"])[0]

# 2: Deploy the deployment.
serve.run(SentimentAnalysisDeployment.bind(), route_prefix="/")

# 3: Query the deployment and print the result.
print(
    requests.get(
        "http://localhost:8000/", params={"text": "Ray Serve is great!"}
    ).json()
)
# {'label': 'POSITIVE', 'score': 0.9998476505279541}
```

## **Why choose Serve?**

---

### **엔드투엔드(End-to-End) ML 기반 애플리케이션을 구축**

많은 ML 서빙 솔루션은 “tensor-in, tensor-out” 방식에 초점을 맞춘다. 즉, 미리 정의된 구조의 엔드포인트 뒤에 ML 모델을 감싸는 형태다. 하지만 머신러닝은 고립된 상태로 존재하는 경우가 드물며, 실제로는 비즈니스 로직이나 데이터베이스 쿼리 같은 웹 로직과 결합되는 것이 중요하다.
Ray Serve의 독특한 점은 **하나의 프레임워크에서 엔드투엔드 분산 서빙 애플리케이션을 직접 구축하고 배포할 수 있다는 점**이다. 여러 ML 모델과 비즈니스 로직, 그리고 Serve의 FastAPI 통합을 활용한 표현력 높은 HTTP 핸들링을 결합해 **전체 애플리케이션을 하나의 Python 프로그램으로 구성할 수 있다**

### **프로그래머블 API를 사용해 여러 모델을 결합**

많은 문제는 단일 머신러닝 모델로 해결되지 않는다. 예를 들어, 이미지 처리 애플리케이션은 사전 처리, 세그멘테이션, 필터링 등 여러 단계로 구성된 파이프라인을 필요로 할 때가 많다. 또한 **모델마다 아키텍처나 프레임워크가 다르고, 필요한 리소스(CPU vs GPU)도 다를 수 있다**.
다수의 다른 솔루션은 YAML이나 기타 구성 언어로 **정적 그래프**를 정의하도록 요구한다. 이는 제한적이고 다루기 어렵다. 반면 Ray Serve는 **프로그래머블 API 기반 멀티 모델 조합**을 지원하며, 서로 다른 모델 호출이 마치 함수 호출처럼 보인다. 모델들은 서로 다른 머신에 배치될 수 있고 서로 다른 리소스를 사용할 수 있지만, 당신은 이를 단지 일반 Python 프로그램처럼 작성할 수 있다.

### **유연하게 확장하고 리소스를 할당**

머신러닝 모델은 연산 집약적이며 운영 비용이 높다. 따라서 어떤 ML 서빙 시스템에서도 요청 부하에 맞게 동적으로 확장/축소하고 비용을 절약하는 것이 중요하다.
Serve는 효율적인 ML 서빙 애플리케이션을 만들기 위한 여러 기본적인 기능을 제공한다.
- 모델의 replica 수를 조절하여 동적으로 리소스를 확장/축소
- 요청 배칭을 통한 벡터화 연산 최적화(GPU에서 특히 중요)
- 유연한 리소스 할당 모델을 통해 제한된 하드웨어에서 여러 모델을 공유

### **특정 프레임워크나 벤더에 종속되는 것을 피할 수 있다**

머신러닝은 빠르게 변화하는 분야이며 새 라이브러리와 모델 아키텍처가 계속 출시된다. 특정 프레임워크에 종속되는 솔루션은 위험하며, 변경 비용과 운영 리스크가 크다. 또 많은 호스팅 솔루션은 단일 클라우드 제공자에 묶여있어 멀티-클라우드 시대에 문제를 일으킬 수 있다.
Ray Serve는 특정 머신러닝 라이브러리나 프레임워크에 묶여있지 않으며, **일반 목적의 확장 가능 서빙 레이어**를 제공한다. Ray 위에 구축되어 있기 때문에 Ray가 실행될 수 있는 모든 환경(노트북, Kubernetes, 주요 클라우드, 베어메탈/on-prem)에서 실행할 수 있다.

## **How can Serve help me as a…**

---

### **데이터 사이언티스트**

Serve는 노트북에서 동작하던 코드를 그대로 클러스터까지 확장하기 쉽게 도와준다. 로컬 머신에서 전체 배포 그래프를 테스트한 뒤 프로덕션 클러스터에 배포할 수 있으며, 복잡한 Kubernetes 개념을 몰라도 된다.

### **ML engineer**

Serve는 배포 확장과 안정적·효율적 실행을 도와 비용을 절감한다. 모델 조합 API를 통해 모델과 비즈니스 로직을 결합한 **엔드투엔드 사용자-facing 애플리케이션**을 구축할 수 있다. 또한 Serve는 Kubernetes에서 네이티브하게 실행되며 운영 오버헤드가 적다.

### **ML platform engineer**

Serve는 확장 가능하고 신뢰할 수 있는 ML 모델 서빙에 특화되어 있으며, ML 플랫폼 스택의 플러그앤플레이 컴포넌트로 사용하기 좋다. 임의의 Python 코드를 지원하므로 **모델 최적화 도구(ONNX, TVM), 모델 모니터링 시스템(Seldon Alibi, Arize), 모델 레지스트리(MLflow, WandB), ML 프레임워크(XGBoost, Scikit-Learn), UI(Gradio, Streamlit), 웹 API(FastAPI, gRPC)**등과 자연스럽게 통합된다.

### **LLM developer**

Serve는 확장 가능한 LLM 애플리케이션을 빠르게 프로토타입하고 배포할 수 있게 한다. LLM 애플리케이션은 **프롬프트 전처리, 벡터 DB 검색, LLM 호출, 응답 검증을 조합**하는 경우가 많다. Serve는 임의의 Python 코드를 지원하므로 이러한 모든 단계를 하나의 Python 모듈로 작성해 빠르게 개발하고 테스트할 수 있다. 프로덕션에 배포하면 각 단계는**사용자 트래픽에 맞춰 독립적으로 autoscale 되어 리소스를 효율적으로 사용**할 수 있다. Serve는 배칭 기능과 어떤 모델 최적화 기술과도 통합할 수 있으며, LLM 앱에서 중요한 스트리밍 응답도 지원한다.

## **How does Serve compare to …**

---

### **TFServing, TorchServe, ONNXRuntime**

Ray Serve는 프레임워크에 구애받지 않으므로 어떤 Python 프레임워크나 라이브러리와도 함께 사용할 수 있다. 데이터 과학자는 특정 ML 프레임워크에 묶여서는 안 되며, 작업에 가장 적합한 도구를 쓸 수 있어야 한다는 철학을 따른다.
프레임워크 특화 솔루션들과 달리 Ray Serve는 특정 모델을 더 빠르게 실행하기 위한 **모델 특화 최적화를 제공하지 않는다. 그러나 사용자가 직접 모델을 최적화하여 Ray Serve에서 실행할 수 있다(예: TorchScript, ONNX Runtime 등).**

### **AWS SageMaker, Azure ML, Google Vertex AI**

Ray Serve는 오픈소스 프로젝트로서, 이러한 호스팅 서비스들의 확장성과 안정성을 사용자의 인프라 환경에 그대로 가져온다. Ray 클러스터 런처를 사용하면 주요 클라우드, Kubernetes, 베어메탈 등 어디든 Ray Serve를 배포할 수 있다.
Ray Serve는 완전한 ML 플랫폼은 아니다. 다른 솔루션과 달리 모델 생애주기 관리나 모델 성능 시각화 기능은 제공하지 않는다. Ray Serve는 **모델 서빙 자체**와 **ML 플랫폼을 구축하기 위한 원시(primitives) 제공**에 초점을 맞춘다.

### **Seldon, KServe, Cortex**

사용자는 노트북에서 개발한 Ray Serve 코드를 dev 환경, 그리고 여러 머신이나 Kubernetes 클러스터로 확장할 수 있으며, 코드 변경이 거의 필요 없다. Kubernetes 클러스터를 직접 관리하지 않아도 쉽게 시작할 수 있다. 배포 시점에 Kubernetes Operator를 사용하면 Ray Serve 애플리케이션을 K8s에 투명하게 배포할 수 있다.

### **BentoML, Comet.ml, MLflow**

많은 도구들은 모델을 독립적으로 서빙 및 확장하는 데 초점을 맞춘다. 반면 Ray Serve는 프레임워크-**agnostic한 모델 조합**에 집중한다. 따라서 어떤 모델 패키징/레지스트리 포맷과도 동작하며, 프로덕션 애플리케이션 구축을 위한 핵심 기능들을 제공한다(최고 수준의 autoscaling, 비즈니스 로직과 자연스럽게 통합 등).
우리는 Serve가 확장성과 성능을 제공하면서도 ML 애플리케이션에 대한 엔드투엔드 제어권을 준다는 점에서 독특하다고 믿는다. 다른 도구로 Serve의 기능을 재현하려면 TensorFlow Serving + SageMaker 같은 여러 프레임워크를 조합하거나, 처리량 개선을 위해 직접 마이크로 배칭 컴포넌트를 구현해야 할 것이다.

## Getting Started

> - 머신러닝 모델을 Ray Serve Deployment로 변환하는 방법
>
> - 로컬에서 HTTP로 Ray Serve 애플리케이션을 테스트하는 방법
>
> - 여러 개의 머신러닝 모델을 하나의 애플리케이션으로 구성(composition)하는 방법
>
> 튜토리얼에서는 두 개의 모델을 사용합니다:
>
> - HuggingFace의 TranslationPipeline (텍스트 번역 모델)
>
> - HuggingFace의 SummarizationPipeline (텍스트 요약 모델)
>
> 필요하다면 어떤 Python 프레임워크의 모델이든 그대로 따라갈 수 있습니다.
> 두 모델을 배포한 후 HTTP 요청을 통해 테스트할 것입니다.

다음 라이브러리를 설치해야 합니다:

```bash
pip install "ray[serve]" transformers requests torch
```

### Run

```text
pip install "ray[serve]" transformers requests torch
Successfully installed MarkupSafe-3.0.3 annotated-doc-0.0.4 anyio-4.12.0 exceptiongroup-1.3.1 fastapi-0.123.5 h11-0.16.0 hf-xet-1.2.0 httptools-0.7.1 huggingface-hub-0.36.0 jinja2-3.1.6 mpmath-1.3.0 networkx-3.2.1 python-dotenv-1.2.1 regex-2025.11.3 safetensors-0.7.0 starlette-0.49.3 sympy-1.14.0 tokenizers-0.22.1 torch-2.8.0 tqdm-4.67.1 transformers-4.57.3 uvicorn-0.38.0 uvloop-0.22.1 watchfiles-1.1.1 websockets-15.0.1
```

## Text Translation Model (Ray Serve 적용 이전)

---

우선 텍스트 번역 모델을 살펴보겠습니다. 아래는 해당 코드입니다:

```python
# File name: model.py
from transformers import pipeline

class Translator:
    def __init__(self):
        # Load model
        self.model = pipeline("translation_en_to_fr", model="t5-small")

    def translate(self, text: str) -> str:
        # Run inference
        model_output = self.model(text)

        # Post-process output to return only the translation text
        translation = model_output[0]["translation_text"]

        return translation

translator = Translator()

translation = translator.translate("Hello world!")
print(translation)
```

이 Python 파일(model.py)은 Translator 클래스를 사용해 영어 텍스트를 프랑스어로 번역합니다.
- Translator의 __init__ 내부에서 self.model 은 t5-small 기반 번역 함수(pipeline)를 로드합니다.
- self.model(text) 를 호출하면 다음 구조의 리스트가 반환됩니다: `[{"translation_text": "..."}]`
- translate 메서드는 이 중 번역된 문자열만 추출하여 반환합니다.

이 스크립트를 로컬에서 실행하면 `"Hello world!" → "Bonjour Monde!"` 로 번역됩니다.

```
python model.py
Bonjour Monde!
```

TranslationPipeline은 단지 예시일 뿐이며, 다른 모든 ML 프레임워크 모델도 동일한 방식으로 적용할 수 있습니다.

## Converting to a Ray Serve Application

---

이제 텍스트 번역 모델을 Ray Serve로 배포하여 스케일링하고 HTTP로 요청을 받을 수 있게 만들겠습니다. 다음은 Translator를 Ray Serve Deployment로 변환하는 과정입니다.

먼저 새 Python 파일을 만들고 아래 모듈을 import 합니다:

```python
from starlette.requests import Request

import ray
from ray import serve
```

그 후 기존 모델 코드를 추가합니다:

```python
from transformers import pipeline

@serve.deployment(num_replicas=2, ray_actor_options={"num_cpus": 0.2, "num_gpus": 0})
class Translator:
    def __init__(self):
        # Load model
        self.model = pipeline("translation_en_to_fr", model="t5-small")

    def translate(self, text: str) -> str:
        # Run inference
        model_output = self.model(text)

        # Post-process output to return only the translation text
        translation = model_output[0]["translation_text"]

        return translation

    async def __call__(self, http_request: Request) -> str:
        english_text: str = await http_request.json()
        return self.translate(english_text)
```

Translator 클래스는 아래 두 가지가 추가되었습니다:
1. @serve.deployment 데코레이터
1. HTTP 요청을 처리하는 __call__ 메서드

`**@serve.deployment**`**데코레이터는**`**Translator**`**클래스를 Ray Serve**`**Deployment**`**객체로 변환**합니다.

각 Deployment는 여러분이 작성한 하나의 파이썬 함수 또는 클래스를 저장하고, 이를 사용해 요청을 처리합니다. @serve.deployment 데코레이터의 파라미터를 사용해 각 Deployment를 독립적으로 스케일링하고 설정할 수 있습니다. 예제에서는 다음과 같은 공통 파라미터들을 설정합니다:
- **num_replicas** : Ray에서 해당 Deployment 프로세스가 몇 개의 복제본으로 실행될지를 결정하는 정수입니다. 요청은 이 복제본들 사이에 로드 밸런싱되므로, 수평 확장이 가능합니다.
- **ray_actor_options** : 각 replica(Actor)에 대한 설정 옵션을 담는 딕셔너리입니다.
  - num_cpus : 각 replica가 예약해야 하는 CPU의 논리적 개수입니다. 소수로 설정해 CPU가 적은 머신에서도 여러 replica를 동시에 배치할 수 있습니다. (한 머신에 CPU 적은데 여러 replica 두는 게 좋은 이유 → I/O 기반 모델(예: 작은 transformer CPU 모델)은 CPU를 계속 100% 쓰지 않으므로 오히려 더 효율적이다.)
  - num_gpus : 각 replica가 예약해야 하는 GPU의 논리적 개수입니다. 이 역시 소수 사용이 가능하며, GPU 리소스가 적은 클러스터에서도 여러 replica를 배치할 수 있습니다.
  - resources : HPU, TPU 같은 GPU가 아닌 하드웨어 가속기 등을 포함한 기타 리소스 요구사항을 지정하는 딕셔너리입니다.

이 모든 파라미터는 선택 사항이므로 필요하지 않으면 생략할 수 있습니다:

```python
...
@serve.deployment
class Translator:
    ...
```

Deployment는 Starlette의 HTTP request 객체를 입력으로 받습니다. 기본적으로, Deployment 클래스의 `__call__` 메서드가 이 요청 객체를 전달받아 실행됩니다. 이 메서드의 반환값은 HTTP response body로 전송됩니다.

따라서 Translator에는 새로운 `**__call__**`**메서드**가 필요합니다. 이 메서드는 들어오는 **HTTP 요청을 처리하기 위해 JSON 데이터를 읽고, 해당 텍스트를 translate 메서드로 전달**합니다. 번역된 텍스트는 반환되어 HTTP 응답으로 전송됩니다. 원시 HTTP 요청을 직접 다루지 않고 싶다면 Ray Serve의 FastAPI 통합을 사용할 수도 있습니다. 자세한 내용은 *[FastAPI HTTP Deployments](https://docs.ray.io/en/latest/serve/http-guide.html)* 문서를 참고하세요.

즉, `__call__`은 “이 Deployment가 HTTP 요청을 받으면 무엇을 할지” 정의하는 메서드이다. FastAPI의 `@app.post("/")` 역할을 대신한다고 보면 된다.

다음으로, Translator Deployment를 생성자에 전달할 인자와 함께 bind 해야 합니다. 이 과정은 로컬에서 실행하거나 프로덕션에서 배포할 수 있는 Ray Serve 애플리케이션을 정의합니다. Translator의 생성자는 인자를 받지 않으므로, bind 메서드를 인자 없이 호출할 수 있습니다:

```python
translator_app = Translator.bind()
```

이제 로컬에서 애플리케이션을 테스트할 준비가 완료되었습니다.

## Running a Ray Serve Application

---

아래는 위에서 만든 전체 Ray Serve 스크립트이다:

```python
# File name: serve_quickstart.py
from starlette.requests import Request

import ray
from ray import serve

from transformers import pipeline

@serve.deployment(num_replicas=2, ray_actor_options={"num_cpus": 0.2, "num_gpus": 0})
class Translator:
    def __init__(self):
        # 모델 로드
        self.model = pipeline("translation_en_to_fr", model="t5-small")

    def translate(self, text: str) -> str:
        # 추론 실행
        model_output = self.model(text)

        # 출력에서 번역 텍스트만 추출
        translation = model_output[0]["translation_text"]

        return translation

    async def __call__(self, http_request: Request) -> str:
        english_text: str = await http_request.json()
        return self.translate(english_text)

translator_app = Translator.bind()
```

로컬에서 테스트하려면 `serve run` CLI 명령어로 스크립트를 실행한다. 이 명령은 `module:application` 형식의 import 경로를 입력으로 받는다. `serve_quickstart.py`가 저장된 디렉토리에서 명령어를 실행해야 해당 애플리케이션을 import할 수 있다:

```
serve run serve_quickstart:translator_app
```

이 명령어는 `translator_app` 애플리케이션을 실행하고 콘솔에 로그를 스트리밍하며 블록된다. `Ctrl-C`로 중단하면 애플리케이션이 종료된다. 이제 HTTP로 모델을 테스트할 수 있다. 기본 접근 URL은 다음과 같다:

```
http://127.0.0.1:8000/
```

영어 텍스트를 담은 JSON 데이터를 POST 요청으로 보낸다. `Translator` 클래스의 `__call__` 메서드는 이 텍스트를 받아 `translate` 메서드로 전달한다. 아래는 “Hello world!”의 번역을 요청하는 클라이언트 스크립트이다:

```python
# File name: model_client.py
import requests

english_text = "Hello world!"

response = requests.post("http://127.0.0.1:8000/", json=english_text)
french_text = response.text

print(french_text)
```

배포를 테스트하려면 먼저 `Translator`가 실행 중인지 확인한다:

```
$ serve run serve_deployment:translator_app
```

Translator가 실행 중일 때, 별도의 터미널에서 클라이언트 스크립트를 실행하면 HTTP 응답을 받을 수 있다:

```
python model_client.py
```

출력 예:

```
Bonjour monde!
```

## Composing Multiple Models

Ray Serve는 여러 Deployment를 하나의 애플리케이션으로 구성할 수 있습니다.

이를 통해 여러 ML 모델 + 비즈니스 로직을 연결된 Pipeline 형태로 처리할 수 있습니다.

## 목표 파이프라인
1. 영문 텍스트 요약
1. 요약된 텍스트를 프랑스어로 번역

## Summarizer (로컬 테스트 코드)

```python
# File name: summary_model.py
from transformers import pipeline

class Summarizer:
    def __init__(self):
        # Load model
        self.model = pipeline("summarization", model="t5-small")

    def summarize(self, text: str) -> str:
        # Run inference
        model_output = self.model(text, min_length=5, max_length=15)

        summary = model_output[0]["summary_text"]
        return summary
```

실행:

```
python summary_model.py
it was the best of times, it was the worst of times .
```

---

## 모델 구성 Application (요약 + 번역)

```python
# File name: serve_quickstart_composed.py
from starlette.requests import Request

import ray
from ray import serve
from ray.serve.handle import DeploymentHandle

from transformers import pipeline

@serve.deployment
class Translator:
    def __init__(self):
        self.model = pipeline("translation_en_to_fr", model="t5-small")

    def translate(self, text: str) -> str:
        model_output = self.model(text)
        translation = model_output[0]["translation_text"]
        return translation

@serve.deployment
class Summarizer:
    def __init__(self, translator: DeploymentHandle):
        self.translator = translator
        self.model = pipeline("summarization", model="t5-small")

    def summarize(self, text: str) -> str:
        model_output = self.model(text, min_length=5, max_length=15)
        summary = model_output[0]["summary_text"]
        return summary

    async def __call__(self, http_request: Request) -> str:
        english_text: str = await http_request.json()
        summary = self.summarize(english_text)

        translation = await self.translator.translate.remote(summary)
        return translation

app = Summarizer.bind(Translator.bind())
```

### remote() 호출 방식

```
self.translator.translate.remote(summary)
```

이는 Translator의 translate 메서드를 **비동기 원격 호출**합니다.

await 를 붙이면 결과를 받아옵니다.

---

## 실행

```
serve run serve_quickstart_composed:app
```

---

## Client 코드

```python
# File name: composed_client.py
import requests

english_text = (
    "It was the best of times, it was the worst of times, it was the age "
    "of wisdom, it was the age of foolishness, it was the epoch of belief"
)
response = requests.post("http://127.0.0.1:8000/", json=english_text)
french_text = response.text

print(french_text)
```

실행 결과:

```
c'était le meilleur des temps, c'était le pire des temps .
```

---

## 결론

이처럼 Ray Serve의 Deployment Composition 기능을 사용하면 ML 파이프라인의 각 단계를 개별 Deployment로 나누고, 각각을 독립적으로 스케일링/구성할 수 있습니다. 더 자세한 내용은 Model Composition 문서를 참고하세요.

---

---

## 운영 관점에서 남긴 결론

모델 코드와 서빙 수명주기를 분리하고, Deployment composition으로 요약과 번역 단계를 독립적으로 운영한다. 설정값을 외우기보다 제어면과 실행면, 워크로드 수명주기, 관측 가능한 성공 조건을 분리해 보는 것이 핵심입니다.

*실환경을 식별할 수 있는 호스트명·IP·내부 경로는 공개용 예시로 일반화했습니다.*
