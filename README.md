# Dotomation (닷동화) · `v1.3`

Dotomation은 사용자의 목적에 따라 **두 가지 운영 모드** 중 하나를 선택해 세팅하고, `dbc-hbin/omp-superkit@0159023e`의 **SUPER/Superstack 워크플로우**와 **역할별 GPT-6.2 Astra v2 서브에이전트 프롬프트**를 **코드·훅·하드 게이트 수준으로 강제**하는 키트다. `python3 tools/dots.py init`을 실행하거나 아래 모드 문서를 보고 **1번(`dots-only`)** 과 **2번(`sub-dots`)** 중 하나를 고른다.

| 선택 | 모드 | 누구의 쿼터를 쓰나 | 핵심 목적 | 문서 / 도구 |
|---|---|---|---|---|
| **1번** | **`dots-only`** | **Dots 자체 쿼터만** (로컬 쿼터 0) | 로컬 세션 개입 없이 Dots가 VM/연결 맥 판별·네이티브 CLI·`dots.py prompt`/`loop` Astra v2 하네스로 끝까지 완주 | [modes/dots-only/README.md](modes/dots-only/README.md) · [RULES.md](modes/dots-only/RULES.md) · [BRIEF.md](modes/dots-only/BRIEF.md) |
| **2번** | **`sub-dots`** | 로컬 메인 세션 + Dots 서브에이전트 | `web-gpt-agent`의 정반대 방향(로컬 세션 → 로컬 허브 → Dots 서브에이전트)으로 **Dots 본체를 경유하지 않고** 직접 실시간 스티어링·Astra v2 ACK/Read-Only 강제·죽음/헛돌기 감시 | [modes/sub-dots/README.md](modes/sub-dots/README.md) · [`tools/subdots.py`](tools/subdots.py) · [PROTOCOL.md](modes/sub-dots/PROTOCOL.md) |

---

## Astra v2 서브에이전트 강제 하네스 (`omp-superkit@0159023e`)

문서(`profiles/astra/*.md`)만 수동으로 복붙하던 방식과 달리, v1.3부터는 [`tools/dots.py`](tools/dots.py)와 [`tools/subdots.py`](tools/subdots.py)가 동일한 컴파일 프로필(`astra_profile_bundle`, `role=worker|reviewer|coordinator`)을 내장해 자동으로 주입·검증한다:

- **Mode 1 (`dots-only`):** `dots.py prompt --role <worker|reviewer|coordinator>`, `dots.py loop claim` 최초 호출 시 `[ASTRA_HARNESS_PROFILE]` 자동 출력(엄격한 [memory-v1](protocol/memory-v1.schema.json) 스키마 비오염), `--require-astra-ack` 계약 시 `loop ack --profile-sha <sha>` / `progress`·`complete` 게이트 강제, `dots_pilot.py brief` 트랙별 브리프 자동 생성.
- **Mode 2 (`sub-dots`):** Reverse MCP `initialize.instructions` 역할 바인딩, `register`/`resume`/`provision --brief`/`[CONTEXT_CAPSULE]` 자동 주입, `--require-astra-ack` 세대(epoch)별 `409 ASTRA_PROFILE_ACK_REQUIRED` 게이트, `--astra-role reviewer --read-only` 계약 시 쓰기/커밋 `403/409 SCOPE_VIOLATION` 차단, 서브에이전트 전용 훅([`subagent-hooks.json`](modes/sub-dots/hooks/subagent-hooks.json) · `subdots.py hook subagent-start|subagent-pre-invocation|subagent-stop`).

---

## 모드 선택하기 (`dots.py init`)

- **1번 `dots-only` 세팅:**
  ```bash
  python3 tools/dots.py init --mode dots-only --dot <DOT_ID> --yes
  ```
  로컬 쿼터를 전혀 쓰지 않고, 작업 전 VM/연결된 로컬 맥 확인 및 Web GPT Agents MCP 금지(네이티브 CLI 강제), Astra v2 전문가 규칙, 최소 변경·무스캐폴딩 규칙이 통합된 브리핑을 출력합니다.
- **2번 `sub-dots` 세팅:**
  ```bash
  python3 tools/dots.py init --mode sub-dots --dot <DOT_ID> --project <PROJ>=<PROJ>-main --yes
  python3 tools/subdots.py hub start --ops-base ~/.dots-ops --strict-steering --require-astra-ack
  ```
  로컬 메인 세션(CLI 또는 `subdots.py mcp-stdio`)에서 Reverse Bridge Hub(`tools/subdots.py`)를 통해 Dots 서브에이전트(`subdots.py agent ...` 또는 Reverse Streamable-HTTP MCP)와 직접 양방향으로 주고받습니다.

---

아래는 공통 직접 우편(`dots.py`), 제로 토큰 파일럿(`dots_pilot.py`), 모니터링(`dots_monitor.py`) 및 워크플로 설명입니다.

로컬 세션이 Dot 본체를 거치지 않고 Dots 서브에이전트에게 일을 보내고, 작업 도중 방향을 바꾸고, 결과를 돌려받게 하는 키트다. 여기에 서브에이전트가 그 우편과 Astra v2 전문 역할을 놓치지 않고 이어서 일하게 붙잡아 두는 서브에이전트 하네스(규칙·게이트·대기·기록·메모리·감시)를 함께 둔다.

소유자가 요청한 4개 표준 라이브러리 도구:
- 읽기 전용 점검 도구 `tools/dots_monitor.py`([모니터링](workflows/monitoring.md))
- 공유 폴더 우편·대기·루프·Astra 프롬프트 도구 `tools/dots.py`([직통 우편](workflows/direct-mail.md#steering-send-wait-reply))
- 개인 AI 토큰 없이 단일 저장소에서 Mode 1 멀티트랙 루프를 자동 구동·감시하는 `tools/dots_pilot.py`([`dots-only`](modes/dots-only/README.md#단일-저장소-제로-토큰-파일럿-toolsdots_pilotpy))
- 2번 모드(`sub-dots`) 역방향 브릿지·강제 하네스 `tools/subdots.py`([`sub-dots`](modes/sub-dots/README.md), [AGENTS.md](AGENTS.md))

### 제로 AI 토큰 단일 저장소 부트스트랩 (`tools/dots_pilot.py`)

Cataly 같은 외부 LLM 코디네이터나 개인 AI 토큰 없이 이 저장소 하나만으로 Mode 1 멀티트랙 루프를 바로 세팅·운용할 수 있습니다(`tools/dots-pilot.example.json` 참고):

```bash
python3 tools/dots_pilot.py init --dot REM17 --project myproj --preset research-3track --require-astra-ack
# 또는 UX -> UI -> 구현 3트랙:
python3 tools/dots_pilot.py init --dot REM17 --project myproj --preset ux-ui-impl-3track --require-astra-ack
python3 tools/dots_pilot.py brief          # 트랙별 Astra v2 서브에이전트 브리프 출력
python3 tools/dots_pilot.py doctor
python3 tools/dots_pilot.py once           # 1회 점검·깨우기(단일 인스턴스 flock + nonce 인증 sentinel)
python3 tools/dots_pilot.py render-service --target launchd  # 서비스 파일 출력만 (자동 설치 없음)
```

## 핵심 1: 본체 중계 없는 비실시간/역방향 스티어링

서브에이전트의 대화는 Dot 밖에서 읽거나 쓸 수 없다. 그래서 로컬 메인 세션과 서브에이전트가 Dot 본체 채팅을 거치지 않고 Mode 2(`subdots.py` 오너 인증 역방향 브릿지) 또는 Mode 1(`local`의 공유 폴더 우편함 `<work root>/.dots-ops/<project>/mail/<agentId>/` + `dots.py loop`)으로 직접 주고받는다.

- **Mode 1 공유 우편/루프:** `send` / `wait` / `reply` / `tick` / `board` / `watch` / `loop`. (`live`, `memory`, `listen`, `watch`, `loops`는 예약 디렉터리. `loops/<DOT>/`는 Astra v2 `agent_profile_seen`·`agent_profile_acked` 상태를 추적해 `memory-v1` 엄격 스키마를 건드리지 않는다)
- **Mode 2 역방향 허브:** `hub start` / `provision --brief` / `contract set --astra-role --read-only --require-astra-ack` / `steer --require-readback` / `await-any` / `mcp-stdio` / `hooks/subagent-hooks.json`.

자세한 규칙: [직통 우편](workflows/direct-mail.md#steering-send-wait-reply), [mail-v1](protocol/mail-v1.schema.json), [Mode 2 프로토콜](modes/sub-dots/PROTOCOL.md).

## 핵심 2: 목표·검증 워크플로 (`SUPER`, `Superstack`, Astra v2)

- **SUPER 워크플로 (`omp-superkit@0159023e` 기준, 기존 `/ulw*` 별칭 호환):** `/super`, `/super-plan`, `/super-run`, `/super-loop`, `/super-graph`, `/super-research`, `/stop-continuation`, `/resume-continuation`, 독립 교차 검토 `/hyperplan`, 결과 중심 `/wish`.
- **브랜치 로컬 `/superstack` (`/superstack off`):** 현재 세션/브랜치 안에서만 기존 전문 스킬을 결과에 맞게 조합해 라우팅하며, 추가 SUPER 목표를 만들지 않고 **자식 서브에이전트(`subagent: true`)에 상속되지 않는다**.
- **엔지니어링·검증 규율:** 근본 원인 추적 + 최소 판별 검사(discriminating check), 최대 3회 서로 다른 접근 후 자신의 간섭 수정만 되돌리고(alternate-source 확인 + blocked 격리), `Refusals are final`(거부 우회 금지, 오너 명시 요청 시에만 프로젝트 규칙 수정), 결론/고위험 `file:line` 우선 무첨가 보고(`EXECUTED` vs `TRACED` vs `Not verified`), UI/시각 작업의 기능+참조 오라클 동시 검증.

문서 안내:
- [텍스트 호출법](workflows/dispatch.md), [목표와 모드별 절차](workflows/goals.md), [전문 작업 19종](workflows/specialists.md), [Astra v2 프로필과 역할별 브리핑](profiles/astra/PROFILE.md)
- [호스트 배치와 승인 경로](workflows/hosts.md), [실시간 작업 기록](workflows/live-status.md), [서브에이전트 세션 메모리](workflows/session-memory.md), [직통 우편함](workflows/direct-mail.md), [실제 작업 여부 모니터링](workflows/monitoring.md), [VM 복구](workflows/vm-watchdog.md)
- [교류 명세와 JSON Schema](protocol/handoff-v1.schema.json), [참조 원본과 구현 범위(0159023e)](SOURCES.md), [기능 매트릭스](FEATURES.md), [모드/역할 예시](examples/mode-cases.md) · [역할 판정 예시](examples/astra-cases.md)

### `dots.py` 자동 업데이트

따로 복사해 둔 `dots.py`는 명령이 끝난 뒤 GitHub의 최신 릴리스를 24시간에 한 번만 확인합니다(`watch`, `update`, `prompt` 제외). 새 버전은 SHA-256을 확인한 뒤 적용하고 `dots.py.bak-<이전 버전>`을 남깁니다. 끄려면 `DOTS_AUTO_UPDATE=0` 또는 `dots.json` `"auto_update": false`를 사용하세요.
