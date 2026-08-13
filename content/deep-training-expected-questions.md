### 망 구성 개요에 대하여 설명하시오
- SKB 초고속 인터넷 망
	- 주요 대도시 34개 국사에서 DS를 통해서 집선한다.
	- 일반 매스 가입자는 OLT/CMTS → AR을 통해, 기업 고객은 FBS를 통해 집선한다.
	- 초고속 인터넷: 국내/국제 연동망, IDC → 동작/서초 CR → DS → AR/FBS → 가입자망 → 고객 PC
- SKB IPTV망
	- 실시간 IPTV: 성수 HeadEnd → B 신규 미디어 전용망 → DS → AR → OLT/CMTS → STB/TV ⇒ Multicast
	- VoD: 전국 33개 CDN → DS → AR → OLT/CMTS → STB/TV
- CATV 
	- 초고속 인터넷:  동작/서초 NSP → 16개 Main SO(이중화 구성) → CATV 가입자망
	- <span color="red"> </span>IPTV: 수원 KDMC → 동작/서초 HR → 16개 Main SO(이중화 구성) → CATV 가입자망 ⇒ Multicast
### 라우트 선택 원리에 대하여 설명하시오
- Recursive Lookup: 다음 홉이 또 다른 라우트로 해석 될 때 반복 조회
- Longest Prefix Match: Prefix가 가장 구체적인 (가장 길게 일치하는) 경로를 우선 선택
- Administrative Distance: 라우팅 신뢰도 정보로, 작을 수록 우선 선택
- Metric: 같은 프로토콜내의 cost 정보로, 작을수록 우선 선택
### 인터넷 다이렉트에 대해 표를 완성하시오
<table>
<colgroup>
<col width="83.65625">
<col width="131.65625">
<col width="183.65625">
<col>
<col>
</colgroup>
<tr>
<td>type</td>
<td>라우팅 스코프</td>
<td>인터넷 서비스 스코프</td>
<td>import community</td>
<td>export community</td>
</tr>
<tr>
<td>IDD</td>
<td>Full Domestic</td>
<td>Only Domestic</td>
<td>110</td>
<td>100,110,130,200</td>
</tr>
<tr>
<td>IDI</td>
<td>Full International + default</td>
<td>Only International</td>
<td>120</td>
<td>300+default</td>
</tr>
<tr>
<td>NSP</td>
<td>Full Domestic + default</td>
<td>Domestic + International</td>
<td>130</td>
<td>100,110,130,200 + default</td>
</tr>
</table>
<empty-block/>