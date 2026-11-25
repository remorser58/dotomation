# Dotomation (닷동화)

로컬 세션이 Dot 본체를 거치지 않고 Dots 서브에이전트에게 일을 보내고, 작업 도중 방향을 바꾸고, 결과를 돌려받게 하는 키트다. 실시간 대화가 아니라 공유 폴더 우편함으로 주고받는 비실시간 스티어링이다. 여기에 서브에이전트가 그 우편을 놓치지 않고 이어서 일하게 붙잡아 두는 서브에이전트 하네스(규칙·대기·기록·메모리·감시)를 함께 둔다.

실행 프로그램이나 OMP 확장이 아니다. 셸, 서비스, 계정, 별도 큐, 패키지 설치가 필요 없다. 예외는 소유자가 요청한 두 파일뿐이다: 읽기 전용 점검 도구 `tools/dots_monitor.py`는 설치·서비스·계정·자격 증명 없이 읽기만 하고([모니터링](workflows/monitoring.md)), 기록·메모리·우편을 다루고 서브에이전트를 대기시키는 `tools/dots.py`는 표준 라이브러리만 쓰며 사람이나 에이전트가 띄운 프로세스가 스스로 끝난다([직통 우편](workflows/direct-mail.md#steering-send-wait-reply), [AGENTS.md](AGENTS.md#the-second-exception-toolsdotspy)).

## 핵심 1: 본체 중계 없는 비실시간 스티어링

서브에이전트의 대화는 Dot 밖에서 읽거나 쓸 수 없다. 그래서 로컬 메인 세션과 서브에이전트가 Dot 본체 채팅을 거치지 않고 `local`의 공유 폴더 우편함 `<work root>/.dots-ops/<project>/mail/<agentId>/`으로 직접 주고받는다.

- 보내기: 메인 세션이 `python3 tools/dots.py send <project> <agentId> <kind> <task>`(본문은 stdin)로 지시·답·리뷰를 넣는다.
- 받기: 서브에이전트는 `python3 tools/dots.py wait <DOT> <agentId>`로 기다리다가 대기 중이면 1초 안에, 작업 중이면 다음 `tick`에서 받는다. 그래서 작업 도중에도 방향을 바꿀 수 있다.
- 돌려주기: 서브에이전트는 `reply`로 `result`·`question`·`block`을 보내고 같은 대기로 이어져 메인 세션의 `answer`·`review`를 받는다. 메인 세션은 `inbox`/`read`로 확인한다.
- 본체는 깨우기만 한다: 에이전트가 대기 중이 아니고 20분 동안 답·읽음 표시·새 기록 줄이 없을 때만 메인 세션이 본체 채팅에 한 줄을 보내고, 본체는 그 에이전트를 깨우기만 한다.
- 코드 인계도 본체를 거치지 않는다: 서브에이전트는 GitHub에 올리지 않고 `rev/*` 커밋이나 `local:<작업 경로>`로 넘기며, 메인 세션이 자기 환경에서 검증한 뒤 올린다.

자세한 규칙: [직통 우편](workflows/direct-mail.md#steering-send-wait-reply), [mail-v1](protocol/mail-v1.schema.json).

## 핵심 2: Dots 서브에이전트 하네스

서브에이전트가 우편을 놓치지 않고, 하는 일이 밖에서 보이고, 멈추면 다시 움직이게 하는 장치다. Dot 루트를 신뢰할 수 없는 경우를 기준으로 만들었다.

- 규칙 주입: `dots.py init`이 각 Dot 루트에 붙일 브리핑을 출력하고, 메모리 파일이 아직 없는 에이전트의 첫 `dots.py` 명령은 규칙부터 출력한다.
- 턴을 끝내지 않는 대기: 맡은 일이 없으면 `wait`로 55분씩 기다리고, 시간 초과나 신호로 끊기면 다시 기다린다. 답할 우편이 남아 있으면 `tick done`이 거부된다.
- 기록과 메모리: `tick` 한 번이 `live/<DOT>/` 기록 줄, `memory/<DOT>/<agentId>.json` 세션 메모리, 안 읽은 우편(`MAIL_FIRST`) 확인을 같이 하고, `board`가 전원 상태를 한 화면에 보여 준다.
- 감시견: 오너가 띄우는 `dots.py watch`가 답이 필요한 우편을 찾아 에이전트를 깨우고, 그래도 답이 없으면 알린다.
- 실제 작업 확인: 읽기 전용 `tools/dots_monitor.py`가 기록을 Web GPT Agent 요청 수와 GitHub 증거에 맞대 본다.
- 역할·모델은 [Astra 프로필과 역할별 브리핑](profiles/astra/PROFILE.md), VM 리셋 복구는 [VM watchdog](workflows/vm-watchdog.md)을 따른다.

세부 동작은 아래 [진행이 보이게 하기](#진행이-보이게-하기)에 있다.

## 빠른 시작: 로컬 한 대

1. 이 저장소를 `local`에 클론한다. 헬퍼는 `tools/dots.py` 한 파일이고 표준 라이브러리만 쓴다.
2. `python3 tools/dots.py init`을 실행한다. 터미널이면 묻는 값에 답한다: 공유 폴더(기본 `~/.dots-ops`, `$DOTS_OPS_BASE`가 있으면 그 폴더), Dot id, 프로젝트와 그 메인 세션 id, `vm`·`local` 밖의 추가 호스트(라벨과 용도), 선택 사항인 wake 명령과 notify 명령. 에이전트가 대신 세팅할 때는 같은 값을 플래그로 넘긴다: `--base --dot ID --project ID=MAIN --host LABEL=PURPOSE --wake-cmd '["python3","/work/bin/dot-chat-post","{dot}","{agent}: mail {id}"]' --yes`. 같은 명령을 다시 실행해도 기존 설정·기록·메모리·우편을 지우거나 덮어쓰지 않고 빠진 항목만 더한다. 단, `--wake-cmd`·`--notify-cmd`를 넘기면 그 명령은 새 값으로 바꾸고 `INIT wake (replaced)`로 알린다. 이미 `<work root>/.dots-ops`를 쓰던 인스턴스는 그 폴더를 `DOTS_OPS_BASE`로 지정한다: `init`을 포함한 모든 명령이 같은 폴더를 본다. `init --base DIR`로 다른 곳에 세팅하면 지정할 `DOTS_OPS_BASE` 줄을 함께 출력한다.
3. 마지막에 출력되는 브리핑을 각 Dot 루트의 대화에 그대로 붙여 넣고, 키트 커밋 SHA를 채운다. 브리핑에는 공유 폴더 경로, 명령, 대기 규칙이 들어 있다.
4. 메인 세션은 `python3 tools/dots.py send <project> <agentId> <kind> <task>`로 보내고(본문은 stdin), 서브에이전트는 `python3 tools/dots.py wait <DOT> <agentId>`로 받는다. 대기 중이면 1초 안에, 작업 중이면 다음 tick에서 받는다. 서브에이전트가 `reply`로 `result`·`question`·`block`을 내면 그 명령이 바로 끝나지 않고 같은 대기로 이어져 메인 세션의 `answer`·`review`를 자기 출력으로 받고, 답해야 할 우편이 남아 있으면 `tick done`이 거부된다: [직통 우편](workflows/direct-mail.md#steering-send-wait-reply).

[교류 명세와 JSON Schema](protocol/handoff-v1.schema.json) · [예시 대화](examples/success.json) · [실패·재개 판정 예](examples/edge-cases.md) · [참조 원본과 구현 범위](SOURCES.md)

### `dots.py` 자동 업데이트

따로 복사해 둔 `dots.py`는 명령이 끝난 뒤 GitHub의 최신 릴리스를 24시간에 한 번만 확인합니다. 새 버전은 SHA-256을 확인한 뒤 적용하고, 이전 파일은 `dots.py.bak-<이전 버전>`으로 남깁니다. 이 저장소를 클론한 `tools/dots.py`는 파일을 바꾸지 않고 `git pull`을 안내하며, `watch`는 확인하지 않습니다.

자동 확인을 끄려면 `DOTS_AUTO_UPDATE=0`을 설정하거나 `<공유 폴더>/dots.json`에 `"auto_update": false`를 추가하세요. 자동 확인을 꺼도 `python3 dots.py update --check`로 지금 확인하거나 `python3 dots.py update`로 직접 업데이트할 수 있습니다.

## 진행이 보이게 하기

- `local`의 공유 폴더 `<work root>/.dots-ops/` 안 `live/<DOT>/`에 총괄·담당·검증자가 각자 한 줄씩 JSONL로 남긴다: 시작, 단계마다(작업 중 최소 15분), 막힘, 종료. 필드와 시점은 [live-record-v1](protocol/live-record-v1.schema.json)과 [실시간 작업 기록](workflows/live-status.md)에 있다.
- 조정자는 `live/<DOT>/roster.json`에 슬롯(담당·역할·모델·작업·상태·마지막 기록 시각)을 유지한다. 30분 넘게 새 줄이 없는 작업 담당은 정체로 보고 직접 확인한 뒤 재배정한다.
- 각 에이전트는 기록을 남길 때마다 `memory/<DOT>/<agentId>.json`에 자기 세션 메모리(목표·지금 하는 일·다음 할 일·결정·막힌 점)를 통째로 덮어쓴다. `dots.py tick` 한 번이 기록 줄·메모리·안 읽은 우편 확인을 같이 하고, 안 읽은 우편은 다른 출력보다 먼저 `MAIL_FIRST`로 나온다. `dots.py board`는 전원의 상태·메모리 요약·우편·정체를 한 화면에 보여 준다. 메인 세션·조정자·리드는 이 파일을 직접 읽고, 정체나 VM 리셋으로 자리를 바꾸면 새 담당이 이전 메모리를 읽고 이어받는다: [세션 메모리](workflows/session-memory.md), [memory-v1](protocol/memory-v1.schema.json).
- 맡은 작업이 없는 서브에이전트는 턴을 끝내지 않고 `dots.py wait <DOT> <agentId>`로 55분간 대기한다. 메인 세션이 `dots.py send`로 보내면 그 줄이 1초 안에 출력되고 `wait`가 끝나므로, 조정자가 깨우지 않아도 다음 지시가 작업 도중에 들어간다. `wait`는 출력한 줄을 읽음으로 표시하므로 같은 지시가 두 번 오지 않고, 시간 초과(exit 3)면 `step`을 남기고 다시 대기하며, 신호로 끊기면(exit 4) 다시 대기한다. 대기 자체는 비용이 없지만 세션 폴링이 최대 5분에 한 번 모델 호출이고, 매 폴링은 그 에이전트의 우편함을 디스크에서 다시 읽는다. 작업을 넘길 때도 같은 대기다: `reply`는 `result`·`question`·`block`을 쓰고 계속 기다려 답을 출력하며(`--no-wait`로 끌 수 있다), 답할 우편이 남아 있으면 `tick done`이 거부된다: [직통 우편](workflows/direct-mail.md#steering-send-wait-reply).
- 오너가 정한 프로젝트(메인 세션 `<project>-main`)에서는 메인 세션이 Dot 본체를 거치지 않고 `local`의 공유 폴더 `<work root>/.dots-ops/<project>/mail/<agentId>/` 우편함으로 서브에이전트와 직접 주고받는다. 메인 세션은 `dots.py send`로 보내고 `inbox`/`read`로 답을 확인하며, 서브에이전트는 `reply`로 답한다. 서브에이전트는 코드를 GitHub에 올리지 않는다. push 경로가 승인된 작업 공간이면 프로젝트의 rev-out 저장소에 `rev/*` 커밋을 push 하고, 아니면 커밋을 그 자리에 두고 `local:<작업 경로>`로 적는다. 메인 세션은 자기 환경에서 게이트별로 검증한 뒤 GitHub에 올리고 PR을 연다. 조정자는 수락 답이 온 최신 결과만 닫는다. 에이전트는 `result`를 보낼 때 `done`을 남기고, 수락 답이 오면 그 답과 GitHub에 올린 SHA를 인용한 `done`을 한 줄 더 남긴다. 에이전트가 대기 중이 아니고, 답도 읽음 표시도 새 live 줄도 없이 20분이 지나면 메인 세션이 본체 채팅에 한 줄을 보내고, 본체는 그 에이전트를 깨우기만 한다: [직통 우편함](workflows/direct-mail.md), [mail-v1](protocol/mail-v1.schema.json).
- Dot 루트를 신뢰할 수 없는 경우를 기준으로 만들었다. 메모리 파일이 아직 없는 에이전트의 첫 `dots.py` 명령은 규칙을 먼저 출력하고(루트가 브리핑을 붙이지 않아도 된다), 답할 우편이 남아 있으면 `tick done`을 거부하며, `dots.py board`의 `MAIL_WAITING`·`CLAIM-STALE`은 로스터가 아니라 우편·기록 줄·대기 표시에서만 나온다. 로스터 값은 `roster=<상태> (the root's claim)`으로만 보여 준다. 오너는 `dots.py watch --loop 5 --expect-sha256 <dots.py의 sha256>`을 직접 띄워 둘 수 있다(명령·간격·장부 경로는 `--wake-cmd`·`--notify-cmd`·`--after-min`·`--notify-after-min`·`--state`로 직접 주면 `dots.json`보다 우선한다: 그 파일은 루트가 쓸 수 있는 폴더에 있고, 실행마다 `WATCH_CONFIG` 한 줄이 값의 출처를 밝힌다): 답이 필요한 우편을 찾아 `after_min`(기본 10분) 뒤 한 번 깨우고, 그 뒤 `notify_after_min`(기본 20분)이 지나도 답이 없으면(읽었든 아니든) 한 번 알리고, 알릴 차례인데 notify 명령이 없으면 `WATCH_ERROR`를 남긴다. 다시 켜도 두 번 울리지 않도록 자기 장부를 `watch/state.json`에 두고, 동작마다 `WATCH_WAKE`·`WATCH_NOTIFY`·`WATCH_STUCK`·`WATCH_HELD`·`WATCH_SKIP`·`WATCH_RESOLVED`·`WATCH_TOOL_CHANGED`·`WATCH_ERROR` 한 줄을 출력하며, 읽음 표시가 찍힌 우편도 답이 없으면 `WATCH_STUCK`을 남기고 알림을 건너뛰지 않는다. 한계는 분명하다: 끝난 서브에이전트를 다시 띄우는 것은 Dot 루트만 할 수 있으므로 `watch`는 설정된 wake 명령을 실행하고, 없으면 `WATCH_ERROR`로 말한다: [Dot 루트를 신뢰할 수 없을 때](workflows/direct-mail.md#if-the-dot-root-is-not-trusted).
- VM 작업은 단계마다 `local`에 체크포인트를 남긴다. 리셋이 의심되면 `state:"blocked"`, `note:"vm reset suspected"`로 남기고, 복구는 마지막 확정 체크포인트의 다음 단계 하나부터 진행한다: [VM watchdog](workflows/vm-watchdog.md).
- 기록은 자기 보고다. 실제로 일하는지는 `python3 tools/dots_monitor.py --config <인스턴스 설정> snapshot`으로 본다(설정 파일은 `tools/dots-monitor.example.json`을 저장소 밖에 복사해 채운다). 각 에이전트의 마지막 기록과 그 계정의 Web GPT Agent 데몬 요청 수(15분·60분)를 맞대고, 기록의 증거 링크(PR·커밋·이슈·댓글·CI 실행)가 GitHub에 실제로 있는지 확인해 WORKING·QUIET·STALLED·BLOCKED·DONE·UNPROVEN·LEGACY로 나눈다. `watch --interval 60`은 상태가 바뀔 때만 `DOTS_STALLED` 같은 한 줄을 출력한다. 읽기만 하며 기록을 고치거나 Dot에 메시지를 보내거나 작업을 배정하지 않는다: [모니터링](workflows/monitoring.md).

## 목표·검증 작업 절차

### 원하는 작업 고르기

- 목표부터 정하기: WISH · 증거를 갖춰 실행: ULW · 계획만: ulw-plan · 승인된 계획 실행: ulw-execute.
- 반복·병렬·조사: ulw-loop · mass-ulw · ulw-research · 독립 관점 교차 검토: hyperplan.
- [텍스트 호출법](workflows/dispatch.md), [목표와 모드별 절차](workflows/goals.md), [전문 작업 19종](workflows/specialists.md), [호스트 배치와 승인 경로](workflows/hosts.md), [실시간 작업 기록](workflows/live-status.md), [서브에이전트 세션 메모리](workflows/session-memory.md), [프로젝트 메인 세션과 서브에이전트 직통 우편함](workflows/direct-mail.md), [실제 작업 여부 모니터링](workflows/monitoring.md), [VM 체크포인트·리셋 복구](workflows/vm-watchdog.md), [상위 프로젝트 기능별 구현 범위와 한계](FEATURES.md), [모드 판정 예시](examples/mode-cases.md).

- 역할을 배정할 때는 [Astra 프로필과 역할별 브리핑](profiles/astra/PROFILE.md)을 같은 커밋에서 읽는다. REM·Ultra·xhigh는 브리핑 이름이지 실행 설정의 증거가 아니다. [역할 판정 예시](examples/astra-cases.md)도 있다.

### 한 건 시작하기

1. 승인된 저장소에서 사용할 **40자리 커밋 SHA**를 정한다. 브랜치명이나 최신판이라는 표현만으로는 버전을 고정할 수 없다.
2. 그 커밋의 [스킬](.agents/skills/dotomation/SKILL.md)과 [공통 실행 규칙](workflows/dispatch.md#confirm-the-run-and-target)을 먼저 읽는다. 조정자는 모든 하위 작업자를 승인된 GPT‑6.2 Astra로 네이티브 경로에서 소환한다: 담당·검증은 `xhigh`, Dot마다 총괄 하나는 `ultra`. 소환이 성공하면 Dots가 실제 모델·강도를 표시하지 않아도 작업을 시작하고, 기록에는 `requested <model> <effort>; runtime does not display effective`로 남긴다. 다른 모델이나 더 낮은 강도로 다시 소환하지 않는다. 작업 전 호스트 이름으로 현재 호스트를 확인하고 대상이 바뀌면 다시 확인한다. Dots 자체 기기 연결로 붙은 컴퓨터는 그 컴퓨터의 네이티브 CLI만 쓴다. 오너가 설치한 이름이 지정된 Web GPT Agent 커넥터는 그 호스트로 가는 승인 경로다([승인 경로](workflows/hosts.md#approved-routes)). Codex(Codex Native2, Codex cloud, Codex 연동 도구)는 어느 호스트에서도 쓰지 않는다.
3. 담당자는 짝 검증자에게 결과를 직접 보낸다. 검증자는 정확히 그 결과를 열어 확인한다. 조정자는 검사 기록뿐 아니라 산출물과 요구 조건을 직접 비교해 종료 여부를 정한다. 별도 실행이나 등록 없이 기존 작업의 메시지/메모만 쓰면 된다.

각 담당자와 검증자에게 같은 버전과 범위, 승인된 작업 대상과 도구 제한을 명시해 직접 전달한다. 수신자는 자기 실행 환경에서 확인한다. 호스트나 승인 경로를 확인할 수 없다면 그 작업을 보류하며 다른 모델, 강도, 호스트로 대체하지 않는다.

```text
Dotomation: dots-superkit/handoff-v1 @ <kit commit SHA>
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

이 키트는 직접 브리핑에 포함해서 쓴다. 파일만으로 서비스 전체나 기존 작업자에게 자동 적용되거나, 백그라운드 전달이 시작되거나, 호스트·권한·모델이 확인되지 않는다. 브리핑 전달, 상대방의 수신 확인, 실제 작업에서의 적용 검증은 각각 구분해서 보고한다.
