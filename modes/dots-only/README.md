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

## 선택적 읽기 도구

`dots-only` 모드에서도 필요하면 오너 측에서 다음 도구를 선택적으로 실행할 수 있습니다.

- **실제 작업 확인 (읽기 전용):** `python3 tools/dots_monitor.py --config <config> snapshot` ([모니터링](../../workflows/monitoring.md))
- **빈 슬롯 유지 감시 (오너 전용):** `python3 tools/dots.py watch --keep-full <DOT> --idle-cmd '<argv JSON>'`
