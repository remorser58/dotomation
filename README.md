# Dots Superkit

Dots에서 목표를 정하고, 계획·실행·검증을 나누며, 담당자와 검증자가 직접 대화하는 작업 키트다. 실행 프로그램이나 OMP 확장이 아니다. 셸, 서비스, 계정, 별도 큐, 패키지 설치가 필요 없다. 유일한 예외는 소유자가 요청한 읽기 전용 점검 도구 `tools/dots_monitor.py` 한 파일이며, 설치·서비스·계정·자격 증명 없이 읽기만 한다([모니터링](workflows/monitoring.md)).

## 원하는 작업 고르기

- 목표부터 정하기: WISH · 증거를 갖춰 실행: ULW · 계획만: ulw-plan · 승인된 계획 실행: ulw-execute.
- 반복·병렬·조사: ulw-loop · mass-ulw · ulw-research · 독립 관점 교차 검토: hyperplan.
- [텍스트 호출법](workflows/dispatch.md), [목표와 모드별 절차](workflows/goals.md), [전문 작업 19종](workflows/specialists.md), [호스트 배치와 승인 경로](workflows/hosts.md), [실시간 작업 기록](workflows/live-status.md), [프로젝트 메인 세션과 서브에이전트 직통 우편함](workflows/direct-mail.md), [실제 작업 여부 모니터링](workflows/monitoring.md), [VM 체크포인트·리셋 복구](workflows/vm-watchdog.md), [상위 프로젝트 기능별 구현 범위와 한계](FEATURES.md), [모드 판정 예시](examples/mode-cases.md).

- 역할을 배정할 때는 [Astra 프로필과 역할별 브리핑](profiles/astra/PROFILE.md)을 같은 커밋에서 읽는다. REM·Ultra·xhigh는 브리핑 이름이지 실행 설정의 증거가 아니다. [역할 판정 예시](examples/astra-cases.md)도 있다.

## 한 건 시작하기

1. 승인된 저장소에서 사용할 **40자리 커밋 SHA**를 정한다. 브랜치명이나 최신판이라는 표현만으로는 버전을 고정할 수 없다.
2. 그 커밋의 [스킬](.agents/skills/dots-superkit/SKILL.md)과 [공통 실행 규칙](workflows/dispatch.md#confirm-the-run-and-target)을 먼저 읽는다. 조정자는 모든 하위 작업자를 승인된 GPT‑6.2 Astra로 네이티브 경로에서 소환한다: 담당·검증은 `xhigh`, Dot마다 총괄 하나는 `ultra`. 소환이 성공하면 Dots가 실제 모델·강도를 표시하지 않아도 작업을 시작하고, 기록에는 `requested <model> <effort>; runtime does not display effective`로 남긴다. 다른 모델이나 더 낮은 강도로 다시 소환하지 않는다. 작업 전 호스트 이름으로 현재 호스트를 확인하고 대상이 바뀌면 다시 확인한다. Dots 자체 기기 연결로 붙은 컴퓨터는 그 컴퓨터의 네이티브 CLI만 쓴다. 오너가 설치한 이름이 지정된 Web GPT Agent 커넥터는 그 호스트로 가는 승인 경로다([승인 경로](workflows/hosts.md#approved-routes)). Codex(Codex Native2, Codex cloud, Codex 연동 도구)는 어느 호스트에서도 쓰지 않는다.
3. 담당자는 짝 검증자에게 결과를 직접 보낸다. 검증자는 정확히 그 결과를 열어 확인한다. 조정자는 검사 기록뿐 아니라 산출물과 요구 조건을 직접 비교해 종료 여부를 정한다. 별도 실행이나 등록 없이 기존 작업의 메시지/메모만 쓰면 된다.

각 담당자와 검증자에게 같은 버전과 범위, 승인된 작업 대상과 도구 제한을 명시해 직접 전달한다. 수신자는 자기 실행 환경에서 확인한다. 호스트나 승인 경로를 확인할 수 없다면 그 작업을 보류하며 다른 모델, 강도, 호스트로 대체하지 않는다.

```text
Dots Superkit: dots-superkit/handoff-v1 @ <kit commit SHA>
work_id / revision: <안정된 작업 ID> / 1
조정자 / 담당자 / 검증자: <역할별 실제 수신자>
목표와 완료 조건: <사람에게 어떤 결과가 보이면 끝인가>
승인 범위 / 제외 대상: <허용된 대상·경로·행동, 하지 않을 일>
실행 규칙: <승인된 작업 대상·연결과 도구 제한> | 같은 SHA의 공통 실행 규칙을 각 역할에 전달하고, 호스트가 확인되지 않으면 보류.
담당 산출물: <경로 또는 메시지 ID> | 필수 검사: <검사 ID와 판정 기준>
직접 교류: 질문, 답, 수정 요청, 검증 결과를 관련 담당자에게 보낸다.
제출: 대상 버전·증거 위치·확인한 결과·미확인 항목. 중복 여부를 먼저 확인한다.
종료: 최신 버전의 모든 필수 검사 PASS와 증거를 조정자가 직접 확인한다.
```

[교류 명세와 JSON Schema](protocol/handoff-v1.schema.json) · [예시 대화](examples/success.json) · [실패·재개 판정 예](examples/edge-cases.md) · [참조 원본과 구현 범위](SOURCES.md)

## 진행이 보이게 하기

- 오사카 공유 폴더 `~/CodeProjects/.dots-ops/`의 `live/<DOT>/`(DOT = REM17·REV18)에 총괄·담당·검증자가 각자 한 줄씩 JSONL로 남긴다: 시작, 단계마다(작업 중 최소 15분), 막힘, 종료. 필드와 시점은 [live-record-v1](protocol/live-record-v1.schema.json)과 [실시간 작업 기록](workflows/live-status.md)에 있다.
- 조정자는 `live/<DOT>/roster.json`에 슬롯(담당·역할·모델·작업·상태·마지막 기록 시각)을 유지한다. 30분 넘게 새 줄이 없는 작업 담당은 정체로 보고 직접 확인한 뒤 재배정한다.
- 오너가 정한 프로젝트(현재 요만치, 메인 세션 `yomanchi-main` = 맥북 OMP)에서는 메인 세션이 REV 본체를 거치지 않고 오사카 `~/CodeProjects/.dots-ops/<project>/mail/<agentId>/` 우편함으로 서브에이전트와 직접 주고받는다. 서브에이전트는 코드를 GitHub에 올리지 않는다. 오사카 작업이면 rev-out 저장소에 `rev/*` 커밋을 push 하고, 미니 작업이면 커밋을 그 자리에 두고 작업 경로를 적는다. 메인 세션은 자기 환경에서 게이트별로 검증한 뒤 GitHub에 올리고 PR을 연다. 조정자는 수락 답이 온 최신 결과만 닫는다. 메인 세션이 보낸 메시지에 답이 없고, 그 에이전트의 새 live 줄도 없이 20분이 지나면, 본체는 그 에이전트를 깨우기만 한다: [직통 우편함](workflows/direct-mail.md), [mail-v1](protocol/mail-v1.schema.json).
- VM 작업은 단계마다 오사카에 체크포인트를 남긴다. 리셋이 의심되면 `state:"blocked"`, `note:"vm reset suspected"`로 남기고, 복구는 마지막 확정 체크포인트의 다음 단계 하나부터 진행한다: [VM watchdog](workflows/vm-watchdog.md).
- 기록은 자기 보고다. 실제로 일하는지는 `python3 tools/dots_monitor.py snapshot`으로 본다. 각 에이전트의 마지막 기록과 그 계정의 Web GPT Agent 데몬 요청 수(15분·60분)를 맞대고, 기록의 증거 링크(PR·커밋·이슈·댓글·CI 실행)가 GitHub에 실제로 있는지 확인해 WORKING·QUIET·STALLED·BLOCKED·DONE·UNPROVEN·LEGACY로 나눈다. `watch --interval 60`은 상태가 바뀔 때만 `DOTS_STALLED` 같은 한 줄을 출력한다. 읽기만 하며 기록을 고치거나 Dot에 메시지를 보내거나 작업을 배정하지 않는다: [모니터링](workflows/monitoring.md).

이 키트는 직접 브리핑에 포함해서 쓴다. 파일만으로 서비스 전체나 기존 작업자에게 자동 적용되거나, 백그라운드 전달이 시작되거나, 호스트·권한·모델이 확인되지 않는다. 브리핑 전달, 상대방의 수신 확인, 실제 작업에서의 적용 검증은 각각 구분해서 보고한다.
