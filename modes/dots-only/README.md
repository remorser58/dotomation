# Mode 1: `dots-only` (닷 자체 쿼터 전용 · `v1.4`)

내 로컬 AI 세션(Antigravity, Claude Code, OMP 등)의 쿼터를 전혀 쓰지 않고 **Dots 자체 쿼터만으로** 작업을 수행할 때 선택하는 모드입니다.

## 언제 선택하나요?

- 로컬 세션에서 실시간 스티어링·인터럽트가 필요 없을 때
- Dots 자체 에이전트들이 스스로 완주하고 결과만 돌려받으면 될 때
- 불필요한 감독/래퍼/폴링 루프 토큰을 쓰지 않고 곧바로 최소 변경으로 끝내고 싶을 때

로컬 메인 세션에서 Dots 서브에이전트를 직접 조종하고(스티어링/인터럽트/죽음·헛돌기 감시/강제 Astra 하네스) 싶다면 [2번 모드: `sub-dots`](../sub-dots/README.md)를 선택하세요.

## 빠른 시작

1. 저장소를 클론한 뒤 모드를 `dots-only`(또는 `1`)로 초기화합니다:
   ```bash
   python3 tools/dots.py init --mode dots-only --dot DOT-A --yes
   # 또는 대화형 실행:
   python3 tools/dots.py init
   # -> 모드 선택에서 1 (dots-only) 선택
   ```
2. `init`이 출력한 브리핑(또는 [BRIEF.md](BRIEF.md))에서 `<kit commit SHA>`를 채워 Dots 대화에 그대로 붙여 넣습니다.
3. 적용 규칙 전문은 [RULES.md](RULES.md)에 있습니다.

## 핵심 규칙 요약

- **Critical Rules:**
  1. 작업 공간이 Dots VM인지 연결된 로컬 맥인지 먼저 확인합니다.
  2. 연결된 로컬 맥에서 작업할 때는 Web GPT Agents MCP를 절대 사용하지 말고 네이티브 CLI 도구를 사용합니다.
- **Coding Philosophy & Safety (`omp-superkit@0159023e` 정렬):**
  - 불필요한 스캐폴딩 없이 즉시 최소 완전 수정을 수행하고, 근본 원인 추적 + 최소 판별 검사(discriminating check) + 최대 3회 서로 다른 접근 재평가를 따릅니다.
  - `Refusals are final`: 도구/호스트/사용자 거부를 우회하지 않으며, 프로젝트 규칙 수정은 오너 명시 요청 시에만 수행합니다.
  - 보고는 결론/고위험 `file:line` 우선, `EXECUTED` vs `TRACED` vs `Not verified` 구분, UI/시각 작업은 기능+시각 참조 오라클을 함께 확인합니다.

## 멀티트랙 자율 루프 + Astra v2 하네스 (Dots 본체 개입 최소화)

외부 AI 세션(`subdots.py`) 없이 **1번 모드(`dots-only`)만으로** 연구 → 온톨로지/명세 → 구현·문항 제작 같은 2~3트랙 파이프라인을 멈춤 없이 돌리면서 **역할별 GPT-6.2 Astra v2 프롬프트**를 자동 주입·강제하려면 [`tools/dots.py`](../../tools/dots.py)의 `prompt` + `loop` 하네스를 사용합니다.

```bash
# 0) 역할별 Astra v2 전문가 프롬프트 확인
python3 tools/dots.py prompt --role <worker|reviewer|coordinator> --mode dots-only

# 1) 상시 목표 계약(Goal Contract) 및 트랙 파이프라인 초기화 (옵션: --require-astra-ack)
python3 tools/dots.py loop init REM17 research --project kice --continuous \
  --astra-role worker --require-astra-ack --feeds-to ontology \
  --objective "Analyze lecture videos and items sequentially; extract verified units"
python3 tools/dots.py loop init REM17 ontology --project kice --continuous \
  --astra-role worker --require-astra-ack --feeds-to authoring \
  --objective "Formalize verified insight units into ontology rules"
python3 tools/dots.py loop init REM17 authoring --project kice --continuous \
  --astra-role worker --require-astra-ack --gate UNIT_TESTS \
  --objective "Implement and test generation algorithms from ontology rules"

# 2) 각 서브에이전트 루프 (본체 보고 없이 순차·자율 완주):
#    claim [--wait 1800] (최초 클레임 시 [ASTRA_HARNESS_PROFILE] 자동 출력; memory-v1 스키마 비오염)
#    -> (필요 시) loop ack ... --profile-sha <sha> (또는 progress/complete에 --profile-sha 전달)
#    -> 로컬 가역 작업 직접 수행 -> progress -> complete (자동 다음 트랙 인계) -> 즉시 다음 claim
#    단건 입력 누락/하드 스톱만 `loop block`으로 격리하고 다른 비차단 작업으로 바로 건너뜀(skip-blocked).
```

## 선택적 읽기·감시 도구

- **실제 작업·데몬 건전성 확인 (읽기 전용):** `python3 tools/dots_monitor.py --config <config> snapshot --daemon-health` ([모니터링](../../workflows/monitoring.md))
- **빈 슬롯·중단 없는 루프 감시 (오너 전용):** `python3 tools/dots.py watch --keep-full <DOT> --fill-cmd '<argv JSON>' --idle-cmd '<argv JSON>'`

## 단일 저장소 제로 토큰 파일럿 (`tools/dots_pilot.py`)

Cataly나 개인 AI 토큰 기반 코디네이터 없이 이 저장소만으로 Mode 1 파이프라인 초기화·역할 브리프 생성·감시·깨우기를 자동화할 수 있습니다(`python3` 표준 라이브러리만 사용, 비공개 상태는 `~/.dotomation-pilot` `0700`/`0600`):

```bash
# 1) 연구->온톨로지->제작 3트랙(KICE형) 또는 UX->UI->구현 3트랙(REMORSWARM형) 초기화
python3 tools/dots_pilot.py init --dot REM17 --project myproj --preset research-3track --require-astra-ack
# 또는: python3 tools/dots_pilot.py init --dot REM17 --project myproj --preset ux-ui-impl-3track --require-astra-ack

# 2) 트랙별 Astra v2 역할 프롬프트가 포함된 바로 붙여넣기용 브리프 출력
python3 tools/dots_pilot.py brief

# 3) 환경·권한·SHA-256 핀 점검 및 1회 파일럿 틱 실행
python3 tools/dots_pilot.py doctor
python3 tools/dots_pilot.py once

# 4) 필요 시 소유자가 직접 launchd/systemd 유닛 템플릿 출력 (자동 설치 안 함)
python3 tools/dots_pilot.py render-service --target launchd
```
