# ORCA v2 Execution Backlog

이 문서는 `ORCA v2 Architecture Blueprint`를 실제 작업 단위로 쪼갠 실행용 백로그다.

## P0. Stop The Bleeding

목표: 지금 당장 시스템이 조용히 망가지지 않게 만든다.

- `baseline_context` 인터페이스 불일치 수정
- 월간 워크플로우의 missing import 수정
- `memory.json`, `accuracy.json`, `hunt_log.json`, `jackal_weights.json`에 atomic write 적용
- 핵심 write 경로에 file lock 또는 single-writer 정책 적용
- broad exception 위치에 structured warning log 추가
- GitHub Actions에 `concurrency` 설정 추가
- 운영 상태와 연구 상태를 최소한 파일 레벨로 분리

## P1. Establish A Real State Layer

목표: JSON 파일 중심 운영에서 벗어난다.

- SQLite 도입
- `runs` 테이블 구현
- `predictions` 테이블 구현
- `outcomes` 테이블 구현
- 기존 `memory.json`을 DB-backed projection으로 대체
- 기존 `accuracy.json`을 DB aggregate view로 대체
- 기존 `hunt_log.json`을 DB event log로 대체

## P2. Harden Agent Contracts

목표: 프롬프트 결과를 "느낌"이 아니라 계약으로 다룬다.

- Hunter output schema 고정
- Analyst output schema 고정
- Devil output schema 고정
- Reporter를 narrative-only renderer로 축소
- `evidence`, `source_quality`, `invalidation_rules`, `confidence_basis` 필드 의무화
- schema validation 실패 시 fallback JSON repair 대신 명시적 retry와 error logging 사용

## P3. Separate Research From Production

목표: 백테스트와 운영 판단의 경계를 만든다.

- backtest run과 production run 저장소 분리
- JACKAL shadow 결과를 ORCA accuracy에서 분리
- policy versioning 도입
- challenger policy shadow run 도입
- promotion gate 문서화

## P4. Build The Evaluation Spine

목표: "정확했나"를 넘어서 "왜 정확했나"를 측정한다.

- prediction registry 도입
- candidate registry 도입
- candidate review / candidate outcome / candidate lesson 루프 도입
- calibration metrics 구현
- regime-sliced metrics 구현
- confidence drift monitoring 구현
- outcome resolution lag metric 구현
- policy comparison dashboard 구현

## P5. Redesign GitHub Actions

목표: GitHub Actions를 데이터 저장소 대용으로 쓰지 않는다.

- `aria-run.yml` 분리
- `jackal-run.yml` 분리
- `outcome-resolver.yml` 신설
- `policy-eval.yml` 신설
- `policy-promote.yml` 신설
- mutable state auto-commit 제거
- report artifact upload 기반으로 전환

## P6. Cost And Security

목표: 비용과 비밀정보를 사후 점검이 아니라 1급 운영 제약으로 취급한다.

- 실제 usage 기반 cost ledger 도입
- mode별 budget ceiling 도입
- crisis mode와 cheap mode 분리
- secret scan을 CI fail condition으로 승격
- boot-time env validation 추가
- report export 전 scrub pass 추가

## Suggested Order

### Week 1

- P0 전부
- SQLite 골격 생성
- `runs` 테이블 도입

### Week 2

- `predictions`, `outcomes` 도입
- ORCA/JACKAL 기록 경로 일부 DB 이관
- agent schema validation 추가

### Week 3

- Reporter 축소
- evaluation spine 초안
- shadow/promotion 경계 구현

### Week 4+

- GitHub Actions 재구성
- dashboard observability 확장
- policy versioning 완성

## Done Definition

아래 기준을 만족해야 v2 전환이 의미 있다.

- 운영 run마다 `run_id`가 존재한다.
- 모든 예측은 `prediction_id`를 가진다.
- outcome 없이 accuracy가 계산되지 않는다.
- JACKAL shadow와 ORCA production metric이 섞이지 않는다.
- mutable state를 main branch에 자동 커밋하지 않는다.
- 어떤 결론이든 evidence chain을 역추적할 수 있다.
