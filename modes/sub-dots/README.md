# Mode 2: `sub-dots` (역방향 `web-gpt-agent` 직결 스티어링 + 강제 하네스)

**2번 모드 (`sub-dots`)** 는 원래 구조(`ChatGPT Web -> web-gpt-agent -> 내 로컬 맥 도구`)를 **정반대 방향**으로 뒤집어, **내 로컬 메인 세션 -> 로컬 역방향 브릿지 허브(`tools/subdots.py`) -> Dots 내부 서브에이전트**로 직접 연결합니다.

Dots 본체(루트 채팅)를 경유해 대화를 중계하지 않으며, **하드 스티어링 게이트·검증 증빙 게이트·외장 Context Capsule·Phoenix 무손실 세대교체·로컬 자율 훅**으로 장시간 작업과 강제성을 동시에 보장합니다.

---

## 아키텍처 (Enforced & Immortal Reverse Bridge v1.2)

```text
+---------------------------+        Unix Socket (0600)         +----------------------------------+
|  로컬 메인 세션            | <==============================> |  Reverse Bridge Hub              |
|  - CLI / mcp-stdio        |   await-any · brief · contract     |  tools/subdots.py hub start      |
|  - hooks/ (Session/Stop)  |                                    |  - SQLite WAL (agents/contracts/ |
|  - Antigravity / OMP      |                                    |    checkpoints/messages/events)  |
+---------------------------+                                    |  - Hard Steering + Readback Gate |
                                                                 |  - Verified Done Scope+Checks    |
                                                                 |  - Context Capsule & Phoenix     |
                                                                 |  - Anti-Death / Anti-Spin        |
                                                                 +----------------+-----------------+
                                                                                  ^
                                                 Loopback HTTP / Reverse MCP      |  (또는 SSH-R / Tunnel)
                                                 /a/<agent_token>/* · /mcp        |
                                                                                  v
                                                                 +----------------+-----------------+
                                                                 |  Dots 내부 서브에이전트           |
                                                                 |  - CLI: register/resume/wait/    |
                                                                 |    checkpoint/hibernate/step/exec|
                                                                 |  - Reverse MCP (+Tool Proxy):    |
                                                                 |    read/patch/exec/git_status    |
                                                                 |  - Git Hooks: pre-commit/push    |
                                                                 +----------------------------------+
```

### 4계층 강제 · 장수 · 맥락 유지 메커니즘

1. **Hard Steering Gate (`--strict-steering` / 계약 기본 활성):**
   - 미확인 `steer`/`task`/`review`/`question`이 있으면 `agent exec`, gated tools(`subdots_read`/`patch`/`exec`), 다음 `step`을 `409 STEERING_ACK_REQUIRED`로 기계적으로 차단합니다.
   - `send`/`steer --require-readback` 메시지는 `--readback "<바뀐 방침 1줄>"` 없이는 ACK 자체가 거부(`400 READBACK_REQUIRED`)됩니다.
   - **Sandwich Injection:** `[HARNESS_INTERVENTION]`·`[STEERING_INPUT]`(인증된 오너 위임 `AUTHORITY: OWNER-DELEGATED LOCAL SESSION` 헤더 포함)은 상단에, `[ACTIVE_STEERING_FOOTER]`는 긴 출력 하단에 반복 출력합니다.
   - **Anti-False-Refusal Guard (`autonomy_guard`, 기본 활성):** 워커가 `reply block`/`question` 또는 `step blocked`에서 "오너/사용자 직접 지시가 아님/비신뢰 허브·도구 출력" 또는 "명시되지 않아 실행 불가" 같은 허위 거부를 내면(단 `git push`/PR/배포/파괴적 삭제/`.git`/심볼릭링크/결제/Codex/자격증명·호스트 확인 누락/`SCOPE_VIOLATION`/외부 비신뢰 입력 프롬프트 인젝션 및 `question`의 일반 기술 질문은 예외로 보존), 로컬 수신함 전달은 유지하면서 `false_refusal_detected` 이벤트를 기록하고 `[AUTONOMY_DIRECTIVE]`를 즉시 반환합니다. `reply`와 `step`은 `spin_window_s` 창 내 **통합 카운터**를 공유하며, 1차 허위 거부는 부모 메시지 `answered` 처리와 스핀 초기화 없이(`UNANSWERED_WORK` 유지) CLI `agent reply`가 대기 hang 없이 즉시 복귀(`auto_resume=True`)해 작업 재개를 강제하고, 창 내 누적 2회차 진입 시 `escalated=True`로 전환되어 `--notify-cmd`를 1회 트리거합니다.
2. **Owner-Pinned Contract & Verified Done Gate (`subdots.py contract set`):**
   - 오너만 설정하는 계약에 `goal`, `--workspace`, `--allow-path`, `--forbid-path`, `--forbid-cmd`, `--allow-cmd-prefix`, `--require-check`, `--rollover-max-calls`, `--idle-hibernate`를 고정합니다.
   - `agent step done` 시 허브가 **실제 `git status` 변경 파일 범위(`409 SCOPE_VIOLATION`)** 와 **마지막 편집 이후 `required_checks` 통과 여부(`409 VERIFICATION_REQUIRED`)** 를 검증합니다.
3. **Context Capsule & Dead-Ends (`agent checkpoint` / `agent resume`):**
   - `agent checkpoint`로 `phase`, `remaining`, `verified_facts`, **`dead_ends`(누적 저장)**, `next_action`을 SQLite에 영구 보존합니다.
   - `register`/`resume`, `wait` 기상, `steer` 차단, 매 N회 호출마다 `[CONTEXT_CAPSULE]`(`AUTONOMY:` 오너 위임 로컬 자율 실행 앵커 포함)을 자동 주입해 Dots 내부 컴팩션에서도 실패 이력과 목표·실행 권한을 잃지 않습니다.
4. **3-State Phoenix Lifecycle (장기 세션 무손실 릴레이 & 동면):**
   - **Hot Keep-Alive:** 안전한 슬라이스 `wait` 타임아웃마다 `[KEEPALIVE_SLICE_DONE]` 재진입 지시를 출력합니다.
   - **Planned Phoenix Rollover:** 피로도(`rollover_max_calls`/`bytes`/`age`) 도달 시 `[ROLLOVER_PREPARE]`를 주입하고 `checkpoint --ready-for-rollover` 후 `--wake-cmd`로 새 epoch 워커를 깨워 `agent resume`으로 바통을 넘깁니다.
   - **Hibernate + On-Demand Wake:** 장기 유휴 시 `agent hibernate`(`state=hibernating`)로 워치독 오탐 없이 동면하고, 로컬에서 `send`/`steer` 시 `--wake-cmd`로 즉시 기상합니다.
5. **우회 차단 도구 프록시 (`hub start --enable-tool-proxy` + `contract --enable-tools`) & Git Hook:**
   - Reverse MCP에 `subdots_read`, `subdots_patch`, `subdots_exec`, `subdots_git_status`를 제공해 파일/셸 실행을 허브 게이트 뒤로 통합할 수 있습니다.
   - `subdots.py install-git-hooks <repo> --agent <agentId>`는 `pre-push`를 영구 차단하고 `pre-commit`에 `agent gate-check`를 연결합니다.
6. **로컬 자율 리액터 & 훅 (`await-any`, `brief`, `hooks/`):**
   - 로컬 메인 세션은 `subdots.py await-any` 백그라운드 대기(토큰 0)나 [hooks.json](hooks/hooks.json) / [run-hook.sh](hooks/run-hook.sh) (`SessionStart`, `PreInvocation`, `Stop`)를 통해 미처리 답장/스핀/롤오버 발생 시 자동 기상·이어달리기를 수행합니다.

---

## 빠른 시작

### 1. 초기화 (Mode 2 선택)
```bash
python3 tools/dots.py init --mode sub-dots --dot DOT-A --project myproj=myproj-main --yes
```

### 2. 오너 측 Reverse Bridge Hub & 계약 세팅
```bash
# ~/.subdots (0700)에 허브 구동 (필요 시 --strict-steering --enable-tool-proxy 추가)
python3 tools/subdots.py hub start --ops-base ~/.dots-ops [--wake-cmd ARGV-JSON] [--notify-cmd ARGV-JSON] [--require-provision] --strict-steering --enable-tool-proxy

# 멀티 에이전트 격리 프로비저닝
python3 tools/subdots.py provision worker-1 --dot DOT-A --project myproj --show-token

# 오너 고정 계약 + 검증/롤오버 정책 부여
python3 tools/subdots.py contract set worker-1 \
  --goal "Target A 수정" --workspace /path/to/repo \
  --allow-path "src/target/**" --forbid-path ".github/**" \
  --forbid-cmd "git\s+push" --require-check "pytest -q" \
  --enable-tools --rollover-max-calls 35 --idle-hibernate 1800

# (선택) 로컬 worktree에 commit/push 가드 훅 설치
python3 tools/subdots.py install-git-hooks /path/to/repo --agent worker-1
```

- **로컬 MCP (`python3 tools/subdots.py mcp-stdio`):** 14개 도구(`subdots_list_agents`, `subdots_agent_status`, `subdots_inbox`, `subdots_mark_read`, `subdots_send`, `subdots_steer`, `subdots_await_reply`, `subdots_events`, `subdots_control`, `subdots_set_contract`, `subdots_get_contract`, `subdots_clear_contract`, `subdots_brief`, `subdots_await_any`) 제공.
- **역방향 HTTP MCP (`/<token>/mcp` 또는 `/a/<agent_token>/mcp`):** 13개 도구(`subdots_register`, `subdots_resume`, `subdots_wait`, `subdots_step`, `subdots_reply`, `subdots_ack`, `subdots_heartbeat`, `subdots_checkpoint`, `subdots_hibernate`, `subdots_read`, `subdots_patch`, `subdots_exec`, `subdots_git_status`) 제공.
- **Antigravity / OMP 훅 연결:** [hooks/hooks.json](hooks/hooks.json) 및 `subdots.py hook <session-start|pre-invocation|stop|doctor>`.

### 3. Dots 서브에이전트 연결 및 작업
서브에이전트에게는 [AGENT-BRIEF.md](AGENT-BRIEF.md)를 붙입니다.
```bash
# 1) 최초 등록 또는 세대교체 복귀
python3 tools/subdots.py agent register worker-1 --dot DOT-A --project myproj
# 또는 재소환 시:
python3 tools/subdots.py agent resume worker-1

# 2) Read-Back ACK 포함 실행
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
