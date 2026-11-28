# Mode 2: `sub-dots` (역방향 `web-gpt-agent` 직결 스티어링 + 강제 하네스 `v1.3`)

**2번 모드 (`sub-dots`)** 는 원래 구조(`ChatGPT Web -> web-gpt-agent -> 내 로컬 맥 도구`)를 **정반대 방향**으로 뒤집어, **내 로컬 메인 세션 -> 로컬 역방향 브릿지 허브(`tools/subdots.py`) -> Dots 내부 서브에이전트**로 직접 연결합니다.

Dots 본체(루트 채팅)를 경유해 대화를 중계하지 않으며, **Astra v2 역할 프롬프트 강제 주입/ACK 게이트·하드 스티어링 게이트·검증 증빙 게이트·외장 Context Capsule·Phoenix 무손실 세대교체·로컬/서브에이전트 자율 훅**으로 장시간 작업과 강제성을 동시에 보장합니다.

---

## 아키텍처 (Enforced & Immortal Reverse Bridge v1.3)

```text
+---------------------------+        Unix Socket (0600)         +----------------------------------+
|  로컬 메인 세션            | <==============================> |  Reverse Bridge Hub              |
|  - CLI / mcp-stdio (+profile)| await-any · brief · contract   |  tools/subdots.py hub start      |
|  - hooks/ (Session/Stop)  |                                    |  - SQLite WAL (agents/contracts/ |
|  - Antigravity / /super   |                                    |    checkpoints/messages/events)  |
+---------------------------+                                    |  - Astra v2 Harness + Read-Only  |
                                                                 |  - Hard Steering + Readback Gate |
                                                                 |  - Verified Done Scope+Checks    |
                                                                 |  - Context Capsule & Phoenix     |
                                                                 +----------------+-----------------+
                                                                                  ^
                                                 Loopback HTTP / Reverse MCP      |  (또는 SSH-R / Tunnel)
                                                 /a/<agent_token>/* · /mcp        |  initialize.instructions
                                                                                  v
                                                                 +----------------+-----------------+
                                                                 |  Dots 내부 서브에이전트           |
                                                                 |  - CLI: register/resume/wait/    |
                                                                 |    ack/checkpoint/hibernate/step |
                                                                 |  - Reverse MCP (+Tool Proxy):    |
                                                                 |    read/patch/exec/git_status    |
                                                                 |  - Hooks: subagent-hooks.json    |
                                                                 +----------------------------------+
```

### 6계층 강제 · 역할 격리 · 장수 · 맥락 유지 메커니즘

1. **Astra v2 서브에이전트 프롬프트 강제 하네스 (`omp-superkit@0159023e` 동기화, v1.3):**
   - **역할별 컴파일 및 다중 경로 자동 주입:** `subdots.py`와 `dots.py`가 동일한 `astra_profile_bundle(role, mode)` (`worker` xhigh, `reviewer` xhigh read-only, `coordinator` ultra)를 내장합니다.
     - Reverse MCP `initialize.instructions` (스코프 토큰 `/a/<agent_token>/mcp`에서는 오너 계약/등록 역할에 맞춤 자동 바인딩)
     - `agent register` / `agent resume` / `provision --brief` 자동 첨부 및 매 `capsule_every_n`회마다 `[CONTEXT_CAPSULE]` 내 `ASTRA_PROFILE:` 콤팩트 앵커 재주입
     - 서브에이전트 훅 ([`hooks/subagent-hooks.json`](hooks/subagent-hooks.json) -> `subdots.py hook subagent-start|subagent-pre-invocation|subagent-stop`; 허브가 꺼진 Mode 1에서도 `subagent-start`/`subagent-stop` 동작)
   - **세대별 하드 ACK 게이트 (`hub start --require-astra-ack` / `contract set --require-astra-ack`):** `--profile-sha <sha>` (`agent ack` 또는 `step`/`reply`/`checkpoint`/`hibernate`/`exec`/`tool_*` 인라인)로 해당 epoch의 Astra v2 프로필을 확인하기 전까지 `409 ASTRA_PROFILE_ACK_REQUIRED`로 작업을 차단하며, Phoenix 세대교체(`epoch` 증가)마다 자동으로 다시 확인을 요구합니다.
   - **오너 고정 역할 및 읽기 전용 검토자 강제 (`contract set --astra-role reviewer [--read-only]`):** 서브에이전트가 스스로 `--role worker`로 등록해도 오너 계약/프로비저닝 역할을 덮어쓸 수 없으며(`--astra-role reviewer`는 자동으로 `--read-only`를 활성화), 읽기 전용 계약에서는 `subdots_patch`, `gate-check --action commit|tool_write`, 허용 프리픽스(`--allow-cmd-prefix`) 없는 `subdots_exec`/`exec_start`/`gate-check --action exec`, dirty 워크트리 상태의 `step done`을 `403/409 SCOPE_VIOLATION`으로 기계적 차단합니다.
2. **Hard Steering Gate (`--strict-steering` / 계약 기본 활성):**
   - 미확인 `steer`/`task`/`review`/`question`이 있으면 `agent exec`, gated tools(`subdots_read`/`patch`/`exec`), 다음 `step`을 `409 STEERING_ACK_REQUIRED`로 기계적으로 차단합니다.
   - `send`/`steer --require-readback` 메시지는 `--readback "<바뀐 방침 1줄>"` 없이는 ACK 자체가 거부(`400 READBACK_REQUIRED`)됩니다.
   - **Sandwich Injection & Anti-False-Refusal Guard:** `[HARNESS_INTERVENTION]`·`[STEERING_INPUT]`은 상단에, `[ACTIVE_STEERING_FOOTER]`는 긴 출력 하단에 반복 출력하며, 비-하드스톱 허위 거부("오너 직접 지시 아님"/"명시되지 않음")는 `[AUTONOMY_DIRECTIVE]`로 즉시 재개시킵니다. (`Refusals are final`: 실제 권한/호스트/사용자 거부는 우회 금지)
3. **Owner-Pinned Contract & Verified Done Gate (`subdots.py contract set`):**
   - `goal`, `--workspace`, `--allow-path`, `--forbid-path`, `--forbid-cmd`, `--allow-cmd-prefix`, `--require-check`, `--astra-role`, `--read-only`, `--require-astra-ack`, `--rollover-max-calls`, `--idle-hibernate`를 오너 소켓으로만 고정합니다.
   - `agent step done` 시 허브가 **실제 `git status` 및 `base_git_head..HEAD` 범위(`409 SCOPE_VIOLATION`)** 와 **마지막 편집 이후 `required_checks` 통과 여부(`409 VERIFICATION_REQUIRED`)** 를 검증합니다.
4. **Context Capsule & Dead-Ends (`agent checkpoint` / `agent resume`):**
   - `agent checkpoint`로 `phase`, `remaining`, `verified_facts`, **`dead_ends`(누적 저장)**, `next_action`을 SQLite에 영구 보존합니다.
5. **3-State Phoenix Lifecycle (장기 세션 무손실 릴레이 & 동면):**
   - **Hot Keep-Alive (`wait` 슬라이스)**, **Planned Phoenix Rollover (`rollover_ready` 우선순위 + `rollover_stalled` + `slot_underfilled` + `SPIN_START_CHURN`)**, **Hibernate + On-Demand Wake**.
6. **우회 차단 도구 프록시 (`--enable-tool-proxy` + `--enable-tools`) & Git Hook & 로컬/서브에이전트 훅:**
   - Reverse MCP에 `subdots_read`, `subdots_patch`, `subdots_exec`, `subdots_git_status`를 통합하며, `subdots.py install-git-hooks`로 `pre-commit` / `pre-push`를 보호합니다.
   - 로컬/서브에이전트 훅: [hooks.json](hooks/hooks.json)(메인 세션용 `session-start`, `pre-invocation`, `stop`), [subagent-hooks.json](hooks/subagent-hooks.json)(서브에이전트용 `subagent-start`, `subagent-pre-invocation`, `subagent-stop`).

---

## 빠른 시작

### 1. 초기화 (Mode 2 선택)
```bash
python3 tools/dots.py init --mode sub-dots --dot DOT-A --project myproj=myproj-main --yes
```

### 2. 오너 측 Reverse Bridge Hub & Astra v2 계약 세팅
```bash
# ~/.subdots (0700)에 허브 구동 (--require-astra-ack 로 Astra v2 세대별 확인 강제)
python3 tools/subdots.py hub start --ops-base ~/.dots-ops [--wake-cmd ARGV-JSON] [--notify-cmd ARGV-JSON] [--require-provision] --strict-steering --require-astra-ack --enable-tool-proxy

# 멀티 에이전트 격리 프로비저닝 + 바로 붙여넣기용 Astra v2 브리프 생성
python3 tools/subdots.py provision worker-1 --dot DOT-A --project myproj --role worker --brief --show-token
python3 tools/subdots.py provision review-1 --dot DOT-A --project myproj --role reviewer --brief --show-token

# 오너 고정 계약 (구현 워커 & 읽기 전용 리뷰어)
python3 tools/subdots.py contract set worker-1 \
  --astra-role worker --require-astra-ack \
  --goal "Target A 수정" --workspace /path/to/repo \
  --allow-path "src/target/**" --forbid-path ".github/**" \
  --forbid-cmd "git\s+push" --require-check "pytest -q" \
  --enable-tools --rollover-max-calls 35 --idle-hibernate 1800

python3 tools/subdots.py contract set review-1 \
  --astra-role reviewer --read-only --require-astra-ack \
  --goal "Target A 독립 감사" --workspace /path/to/repo --enable-tools

# (선택) 로컬 worktree에 commit/push 가드 훅 설치
python3 tools/subdots.py install-git-hooks /path/to/repo --agent worker-1
```

- **로컬 MCP (`python3 tools/subdots.py mcp-stdio`):** 15개 도구(`subdots_list_agents`, `subdots_agent_status`, `subdots_inbox`, `subdots_mark_read`, `subdots_send`, `subdots_steer`, `subdots_await_reply`, `subdots_events`, `subdots_control`, `subdots_set_contract`, `subdots_get_contract`, `subdots_clear_contract`, `subdots_brief`, `subdots_await_any`, `subdots_astra_profile`) + `initialize.instructions` 제공.
- **역방향 HTTP MCP (`/<token>/mcp` 또는 `/a/<agent_token>/mcp`):** `initialize.instructions` + 13개 도구(`subdots_register`, `subdots_resume`, `subdots_wait`, `subdots_step`, `subdots_reply`, `subdots_ack`, `subdots_heartbeat`, `subdots_checkpoint`, `subdots_hibernate`, `subdots_read`, `subdots_patch`, `subdots_exec`, `subdots_git_status`, `--profile-sha` 지원) 제공.
- **Antigravity / OMP 훅 연결:** [hooks/hooks.json](hooks/hooks.json) 및 [hooks/subagent-hooks.json](hooks/subagent-hooks.json) -> `subdots.py hook <session-start|pre-invocation|stop|subagent-start|subagent-pre-invocation|subagent-stop|doctor>`.

### 3. Dots 서브에이전트 연결 및 작업
서브에이전트에게는 [AGENT-BRIEF.md](AGENT-BRIEF.md)(또는 `subdots.py provision --brief`)를 붙입니다.
```bash
# 0) (선택) Astra v2 프롬프트 직접 출력
python3 tools/subdots.py prompt --role worker

# 1) 최초 등록 또는 세대교체 복귀
python3 tools/subdots.py agent register worker-1 --dot DOT-A --project myproj --role worker
# 또는 재소환 시:
python3 tools/subdots.py agent resume worker-1

# 2) Astra v2 SHA + Read-Back ACK 포함 실행
python3 tools/subdots.py agent ack worker-1 --profile-sha <sha>
python3 tools/subdots.py agent exec worker-1 --ack <msg_id> --readback "core.py만 수정하고 기존 테스트 유지" -- pytest -q

# 3) 구조화 체크포인트 + Dead-Ends 저장 (롤오버 시 --ready-for-rollover)
python3 tools/subdots.py agent checkpoint worker-1 --task T1 --phase coding \
  --dead-end "동기 lock 중 wait 호출 금지" --next "unit test 실행"

# 4) 진행/완료 보고 (계약 스코프 & 필수 검증 통과 여부 자동 검사)
python3 tools/subdots.py agent step worker-1 done --task T1 --note "verified" --evidence "pytest:passed" --goal "Fix" --doing "Done" --next "Wait"
```

### 4. 로컬 세션에서 스티어링 / 자율 리액터
```bash
# Read-Back 강제 방향 전환
python3 tools/subdots.py steer worker-1 --interrupt --require-readback --text "src/other.py 건드리지 말고 src/core.py만 고쳐"

# 전체 요약 브리프 / 토큰 0 이벤트 리액터 대기
python3 tools/subdots.py brief
python3 tools/subdots.py await-any --timeout 1800 --json
```

세부 프로토콜 계약은 [PROTOCOL.md](PROTOCOL.md)를 참고하세요.
