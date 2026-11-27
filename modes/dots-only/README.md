# Mode 1: `dots-only` (닷 자체 쿼터 전용)

내 로컬 AI 세션(Antigravity, Claude Code, OMP 등)의 쿼터를 전혀 쓰지 않고 **Dots 자체 쿼터만으로** 작업을 수행할 때 선택하는 모드입니다.

## 언제 선택하나요?

- 로컬 세션에서 실시간 스티어링·인터럽트가 필요 없을 때
- Dots 자체 에이전트들이 스스로 완주하고 결과만 돌려받으면 될 때
- 불필요한 감독/래퍼/폴링 루프 토큰을 쓰지 않고 곧바로 최소 변경으로 끝내고 싶을 때

로컬 메인 세션에서 Dots 서브에이전트를 직접 조종하고(스티어링/인터럽트/죽음·헛돌기 감시) 싶다면 [2번 모드: `sub-dots`](../sub-dots/README.md)를 선택하세요.

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
- **Coding Philosophy:** 계획/RFC/승인 게이트/래퍼/감독 하네스 같은 불필요한 절차적 스캐폴딩을 추가하지 않고 요청된 작업부터 바로 시작해 최소한의 완전한 변경만 수행합니다.
- **Safety & Core Operating Instructions:** 실제 사용자/운영자 흐름으로 검증하고, 관련 없는 기존 작업은 보존하며, 파괴적 작업이나 push/merge/release/deploy 전에는 명시적 승인을 받습니다.

## 멀티트랙 자율 루프 (Dots 본체 개입 최소화)

외부 AI 세션(`subdots.py`) 없이 **1번 모드(`dots-only`)만으로** 연구 → 온톨로지/명세 → 구현·문항 제작 같은 2~3트랙 파이프라인을 멈춤 없이 돌리려면 [`tools/dots.py`](../../tools/dots.py)의 `loop` 하네스를 사용합니다.

```bash
# 1) 상시 목표 계약(Goal Contract) 및 트랙 파이프라인 초기화 (외부 세션 0)
python3 tools/dots.py loop init REM17 research --project kice --continuous \
  --feeds-to ontology --objective "Analyze lecture videos and items sequentially; extract verified units"
python3 tools/dots.py loop init REM17 ontology --project kice --continuous \
  --feeds-to authoring --objective "Formalize verified insight units into ontology rules"
python3 tools/dots.py loop init REM17 authoring --project kice --continuous \
  --objective "Implement and test generation algorithms from ontology rules" --gate UNIT_TESTS

# 2) 각 서브에이전트 루프 (본체 보고 없이 순차·자율 완주):
#    claim [--wait 1800] -> 로컬 가역 작업 직접 수행 -> progress -> complete (자동 다음 트랙 인계) -> 즉시 다음 claim
#    단건 입력 누락/하드 스톱만 `loop block`으로 격리하고 다른 비차단 작업으로 바로 건너뜀(skip-blocked).
```

## 선택적 읽기·감시 도구

`dots-only` 모드에서도 오너 측에서 다음 도구를 선택적으로 실행할 수 있습니다.

- **실제 작업·데몬 건전성 확인 (읽기 전용):** `python3 tools/dots_monitor.py --config <config> snapshot --daemon-health` ([모니터링](../../workflows/monitoring.md))
- **빈 슬롯·중단 없는 루프 감시 (오너 전용):** `python3 tools/dots.py watch --keep-full <DOT> --fill-cmd '<argv JSON>' --idle-cmd '<argv JSON>'` (로스터가 120분 이상 오래되면 live 기록 기준으로 빈/정체 레인을 판별하고, 실패한 깨우기도 `--wake-retries`만큼 재시도)

## 단일 저장소 제로 토큰 파일럿 (`tools/dots_pilot.py`)

Cataly나 개인 AI 토큰 기반 코디네이터 없이 이 저장소만으로 Mode 1 파이프라인 초기화·감시·깨우기를 자동화할 수 있습니다(`python3` 표준 라이브러리만 사용, 비공개 상태는 `~/.dotomation-pilot` `0700`/`0600`):

```bash
# 1) 연구->온톨로지->제작 3트랙(KICE형) 또는 UX->UI->구현 3트랙(REMORSWARM형) 초기화
python3 tools/dots_pilot.py init --dot REM17 --project myproj --preset research-3track
# 또는: python3 tools/dots_pilot.py init --dot REM17 --project myproj --preset ux-ui-impl-3track

# 2) 환경·권한·SHA-256 핀 점검 및 1회 파일럿 틱 실행
python3 tools/dots_pilot.py doctor
python3 tools/dots_pilot.py once

# 3) 필요 시 소유자가 직접 launchd/systemd 유닛 템플릿 출력 (자동 설치 안 함)
python3 tools/dots_pilot.py render-service --target launchd
```
