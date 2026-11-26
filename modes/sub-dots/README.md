# Mode 2: `sub-dots` (역방향 `web-gpt-agent` 직결 스티어링 + 하네스)

**2번 모드 (`sub-dots`)** 는 원래 구조(`ChatGPT Web -> web-gpt-agent -> 내 로컬 맥 도구`)를 **정반대 방향**으로 뒤집어, **내 로컬 메인 세션 -> 로컬 역방향 브릿지 허브(`tools/subdots.py`) -> Dots 내부 서브에이전트**로 직접 연결합니다.

Dots 본체(루트 채팅)를 경유해 대화를 중계하지 않으며, 서브에이전트가 혼자 죽거나 헛도는 일을 하네스·감시견이 즉시 감지·차단·복구합니다.

---

## 아키텍처 (Reverse `web-gpt-agent`)

```text
+---------------------------+        Unix Socket (0600)         +---------------------------------+
|  로컬 메인 세션            | <==============================> |  Reverse Bridge Hub             |
|  - CLI (subdots.py)       |   또는 stdio MCP (mcp-stdio)       |  tools/subdots.py hub start     |
|  - Antigravity/Claude/OMP |                                    |  - SQLite WAL (~/.subdots,0700) |
+---------------------------+                                    |  - Two-Phase Delivery           |
                                                                 |  - Epoch Fencing                |
                                                                 |  - Anti-Death Lease Watchdog    |
                                                                 |  - Anti-Spin Intervention       |
                                                                 +----------------+----------------+
                                                                                  ^
                                                 Loopback HTTP / Reverse MCP      |  (또는 SSH-R / Tunnel)
                                                 /<token>/agent/* · /<token>/mcp  |
                                                                                  v
                                                                 +----------------+----------------+
                                                                 |  Dots 내부 서브에이전트           |
                                                                 |  - CLI: subdots.py agent ...    |
                                                                 |  - MCP: subdots_wait/step/reply |
                                                                 |  - Exec: subdots.py agent exec  |
                                                                 +---------------------------------+
```

### 왜 `dots.py` 파일 폴링만으로 부족한가?

1. **즉시 깨어나는 양방향 역방향 푸시:** `agent wait`/`subdots_wait`가 허브의 `Condition`에 걸려 있어 로컬 세션에서 `send`/`steer`/`ask`를 치면 10ms 안에 깨어나 지시가 전달됩니다.
2. **Every-Call Piggyback (`web-gpt-agent`의 `offerToolInput` 역방향):** 서브에이전트가 `wait`, `step`, `heartbeat`, `reply`, `exec`, 역방향 MCP `tools/call` 중 **무엇을 호출하든** 미전달 지시(`queued`/`offered`)가 `[STEERING_INPUT]` 블록으로 함께 실려 나옵니다.
3. **2단계 전달 보장 & 응답 강제:** `queued -> offered -> acked -> answered` 상태 머신을 가지며, 미확인 메시지는 `reoffer_s`(기본 45초)마다 재제시되고 `max_offers` 초과 시 `unacked_failed` 알림이 발생합니다. 답할 지시가 남아 있으면 `step done`이 거부됩니다.
4. **실행 중 인터럽트 (`agent exec`):** 서브에이전트가 명령을 실행하는 도중에도 로컬 세션이 `steer --interrupt`, `pause`, `cancel`을 보내면 감시 스레드가 프로세스 그룹(`os.killpg`)을 즉시 `SIGTERM -> SIGKILL`로 중단시키고 새 지시를 출력합니다.
5. **혼자 죽기 방지 (Anti-Death Watchdog):** 연결 및 Lease TTL(`--lease-ttl`)을 백그라운드 워치독이 추적해 `stalled -> dead` 전이 시 지수 백오프로 `--wake-cmd`를 실행하고, 복구되지 않으면 `--notify-cmd`로 알립니다.
6. **혼자 헛돌기 차단 (Anti-Spin Detector):**
   - `SPIN_COMMAND_LOOP`: 동일 명령+동일 `stderr_hash` 연속/반복 실패(기본 3회)
   - `SPIN_NO_PROGRESS`: 동일 `task/doing/note/evidence` 진전 없는 `step` 반복(기본 4회)
   - `SPIN_POLL_THRASH`: 정상 chunked `wait`가 아닌 초단기 빈 폴 폭주(기본 60초 내 12회)
   - 감지 즉시 `spinning` 전환 + 최우선 `[HARNESS_INTERVENTION]` 메시지 주입 + `--notify-cmd` 알림.
7. **좀비 에이전트 차단 (Epoch Fencing):** 재등록 시 `epoch`가 증가하고 새 `session_token`이 발급되므로 죽었다 살아난 이전 프로세스는 `409 STALE_EPOCH`로 차단됩니다.

---

## 빠른 시작

### 1. 초기화 (Mode 2 선택)
```bash
python3 tools/dots.py init --mode sub-dots --dot DOT-A --project myproj=myproj-main --yes
```

### 2. 오너 측 Reverse Bridge Hub 시작
```bash
# ~/.subdots (0700)에 SQLite WAL과 소켓/HTTP 서버 구동
python3 tools/subdots.py hub start --ops-base ~/.dots-ops

# 상태 확인
python3 tools/subdots.py hub status
```
- **로컬 MCP 연결 (Antigravity/Claude/OMP 등):** `python3 tools/subdots.py mcp-stdio`
- **Dots VM에서 접속해야 할 때:** `ssh -R <원격포트>:127.0.0.1:<허브포트>` 또는 터널을 열고 `--public-host <호스트>`와 `SUBDOTS_HUB_URL=http://127.0.0.1:<원격포트>/<token>`을 전달하세요.

### 3. Dots 서브에이전트 연결 및 작업
서브에이전트에게는 [AGENT-BRIEF.md](AGENT-BRIEF.md)를 붙입니다.
```bash
# 1) 등록 (fencing epoch & session_token 발급)
python3 tools/subdots.py agent register worker-1 --dot DOT-A --project myproj

# 2) 대기 (로컬 세션 지시 수신 즉시 0 종료)
python3 tools/subdots.py agent wait worker-1 --timeout 3300

# 3) 명령 실행 (로컬에서 steer --interrupt 보내면 즉시 중단)
python3 tools/subdots.py agent exec worker-1 -- pytest -q

# 4) 진행 보고 (미확인 스티어링이 최상단에 실려 나옴)
python3 tools/subdots.py agent step worker-1 step --task T1 --note "tests ok" --evidence "pytest:passed" --goal "Fix bug" --doing "Testing" --next "Reply"

# 5) 결과 보고 후 바로 다음 지시 대기 (result/question/block은 답변 올 때까지 자동 대기)
python3 tools/subdots.py agent reply worker-1 result T1 --re <msg_id> --ref rev/fix-1 --text "완료"
```

### 4. 로컬 세션에서 스티어링 / 제어
```bash
# 연결된 에이전트 목록
python3 tools/subdots.py agents

# 지시 보내고 응답까지 대기 (동기 Q&A)
python3 tools/subdots.py ask worker-1 --task T1 --text "먼저 테스트부터 돌려줘"

# 작업 중 실시간 방향 전환 (실행 중인 agent exec 즉시 인터럽트)
python3 tools/subdots.py steer worker-1 --interrupt --text "그 파일 말고 src/core.py만 수정해"

# 일시정지 / 재개 / 취소 / 턴 해제 / 이벤트 스트림
python3 tools/subdots.py pause worker-1
python3 tools/subdots.py resume worker-1
python3 tools/subdots.py events worker-1 --follow
python3 tools/subdots.py release worker-1
```

세부 프로토콜 계약은 [PROTOCOL.md](PROTOCOL.md)를 참고하세요.
