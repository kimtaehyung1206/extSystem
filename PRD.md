# 재난·기상정보 연계시스템 PRD

| 항목 | 내용 |
|---|---|
| 문서명 | 재난·기상정보 연계시스템 요구사항 정의서 (PRD) |
| 버전 | v1.0 |
| 작성일 | 2026-08-14 |
| 시스템 코드 | `extSystem` (External Data Interface System) |
| 기술스택 | Spring Boot 3.x (Java 21), PostgreSQL 17.11 |

---

## 1. 개요

### 1.1 목적

기상청·행정안전부·환경부·산림청 등 **외부 재난/기상 기관의 원천 데이터를 수집·정제·표준화**하여, 내부 서비스가 단일한 규격으로 재난정보를 조회할 수 있도록 하는 **연계(Interface) 플랫폼**을 구축한다.

### 1.2 핵심 요구사항 요약

| # | 요구사항 | 설명 |
|---|---|---|
| R-1 | 7종 재난정보 연계 | 태풍(경로 포함), 폭염, 홍수, 산사태, 황사, 낙뢰, 지진 |
| R-2 | 행정구역 3단계 제공 | 시도(2단계 상위) → 시군구 → 읍면동 단위까지 매핑 및 조회 |
| R-3 | 주기 연계 | 재난유형별 스케줄에 따른 배치 수집 |
| R-4 | 실시간 연계 API | 외부/내부 요청 시 즉시 원천 조회(On-Demand Pull) |
| R-5 | 재시도 정책 | 실패 시 최대 3회 즉시 재시도 → 3회 초과 실패 시 **10분 후 재시도** |
| R-6 | 연계 로그 관리 | 요청/응답/오류/성능 전 구간 로그 적재 및 조회 |

### 1.3 범위

**포함(In-Scope)**

- 원천 API 수집 어댑터, 데이터 표준화/정규화, 행정구역 매핑
- 스케줄러 기반 주기 연계, 실시간 연계 REST API
- 재시도/서킷브레이커, 연계 이력 및 로그 관리, 운영 모니터링 API

**제외(Out-of-Scope)**

- 관리자 화면(UI) — 별도 프로젝트에서 본 시스템의 API를 소비
- 재난 예측 모델링, 알림(Push/SMS) 발송
- 원천기관 인증키 발급 행정 절차

### 1.4 용어 정의

| 용어 | 정의 |
|---|---|
| 연계(Interface) | 외부 원천 시스템에서 데이터를 수집하여 내부 표준 스키마로 적재하는 행위 |
| 원천(Source) | 데이터를 제공하는 외부 기관 시스템 (기상청 API 허브 등) |
| 연계 작업(Job) | 하나의 원천 + 하나의 재난유형 조합에 대한 수집 단위 |
| 실행(Execution) | 연계 작업의 1회 수행 인스턴스 |
| 법정동코드 | 행정표준코드관리시스템 10자리 코드. 시도(2) + 시군구(3) + 읍면동(3) + 리(2) |
| 격자(Grid) | 기상청 동네예보 5km 격자 좌표계 (nx, ny) |

---

## 2. 시스템 아키텍처

### 2.1 구성도

```
┌─────────────────────── 외부 원천(Source Systems) ───────────────────────┐
│ 기상청 API허브 │ 행안부 재난문자 │ 한강홍수통제소 │ 산림청 │ 에어코리아 │
└──────────┬──────────────────────────────────────────────────────────────┘
           │ HTTPS (REST/XML/JSON)
┌──────────▼──────────────────────────────────────────────────────────────┐
│                      extSystem (Spring Boot)                            │
│                                                                          │
│  ┌────────────────┐   ┌──────────────────┐   ┌───────────────────────┐  │
│  │ Scheduler      │   │ Collector        │   │ Interface API         │  │
│  │ (ShedLock)     │──▶│  - SourceAdapter │◀──│  - 실시간 수집 요청    │  │
│  │  주기 연계 트리거│   │  - Parser        │   │  - 조회 API           │  │
│  └────────────────┘   │  - Normalizer    │   │  - 운영/로그 API      │  │
│                       │  - RegionMapper  │   └───────────────────────┘  │
│  ┌────────────────┐   └────────┬─────────┘                              │
│  │ Retry Manager  │            │                                        │
│  │ - 즉시 3회      │◀───────────┤                                        │
│  │ - 10분 지연 재시도│           ▼                                        │
│  └────────────────┘   ┌──────────────────┐   ┌───────────────────────┐  │
│                       │ Persistence      │   │ Log Manager           │  │
│                       │ (JPA + JDBC Bulk)│   │ (실행이력/상세/오류)   │  │
│                       └────────┬─────────┘   └──────────┬────────────┘  │
└────────────────────────────────┼───────────────────────────┼────────────┘
                                 ▼                           ▼
                    ┌──────────────────────────────────────────────┐
                    │           PostgreSQL 17.11                   │
                    │  표준코드 / 행정구역 / 재난데이터(파티션)      │
                    │  연계정의 / 실행이력 / 로그(파티션)           │
                    └──────────────────────────────────────────────┘
```

### 2.2 기술 스택

| 구분 | 기술 | 버전/비고 |
|---|---|---|
| Language | Java | 21 (LTS) |
| Framework | Spring Boot | 3.3.x |
| Web | Spring Web MVC + WebClient | 원천 호출은 WebClient(Reactor Netty) |
| ORM | Spring Data JPA (Hibernate 6) | 대량 적재는 JdbcTemplate `batchUpdate` |
| Migration | Flyway | `V{n}__{desc}.sql` |
| Scheduler | Spring Scheduling + **ShedLock** | 다중 인스턴스 중복 실행 방지 (DB Lock) |
| Resilience | Resilience4j | Retry / CircuitBreaker / RateLimiter |
| Cache | Caffeine (L1) + Redis (L2, 선택) | 행정구역·격자 매핑 캐시 |
| DB | **PostgreSQL 17.11** | 파티셔닝, JSONB, BRIN |
| GIS | PostGIS 3.4 (확장) | 태풍 경로/영향반경 공간연산 |
| Doc | springdoc-openapi | Swagger UI |
| Monitor | Actuator + Micrometer → Prometheus | |
| Build | Gradle 8.x (Kotlin DSL) | |

### 2.3 패키지 구조

```
com.ext.system
├── common          // 응답 규격, 예외, 코드 Enum, 유틸
├── config          // WebClient, Scheduler, ShedLock, Resilience4j, Flyway
├── region          // 행정구역/격자 매핑 도메인
│   ├── domain, repository, service
├── interfacing     // 연계 엔진 (핵심)
│   ├── definition  // 연계정의(IfDefinition) CRUD
│   ├── executor    // InterfaceExecutor, RetryManager
│   ├── adapter     // 원천별 SourceAdapter 구현체
│   │   ├── kma     // 태풍/폭염/황사/낙뢰/지진
│   │   ├── hrfco   // 홍수
│   │   └── forest  // 산사태
│   ├── normalizer  // 원천 → 표준 DTO 변환
│   └── log         // 연계 로그 적재/조회
├── disaster        // 재난 표준 도메인 7종
│   ├── typhoon, heatwave, flood, landslide, dust, lightning, earthquake
└── api             // 외부 노출 REST Controller
    ├── v1.disaster // 재난정보 조회
    ├── v1.trigger  // 실시간 연계 요청
    └── v1.admin    // 연계정의/로그/모니터링
```

---

## 3. 연계 대상 데이터

### 3.1 원천 목록

| 코드 | 재난유형 | 원천 기관/시스템 | 프로토콜 | 제공 최소단위 |
|---|---|---|---|---|
| `TYPHOON` | 태풍 | 기상청 API 허브 (태풍정보/예상경로) | REST/JSON | 위경도 중심 + 반경 |
| `HEATWAVE` | 폭염 | 기상청 기상특보 + 체감온도 | REST/XML | 특보구역(시군구) |
| `FLOOD` | 홍수 | 한강홍수통제소(HRFCO) 수위/홍수예보 | REST/JSON | 관측소(지점) |
| `LANDSLIDE` | 산사태 | 산림청 산사태정보시스템 (예측/경보) | REST/JSON | 읍면동 |
| `YELLOWDUST` | 황사 | 기상청 황사특보 + 에어코리아 PM10 | REST/XML | 특보구역/측정소 |
| `LIGHTNING` | 낙뢰 | 기상청 낙뢰 관측자료 | REST/JSON | 위경도(점) |
| `EARTHQUAKE` | 지진 | 기상청 지진정보/지진통보문 | REST/XML | 진앙 위경도 |

> **원칙**: 원천이 위경도 또는 지점 단위로만 제공하는 경우(태풍/낙뢰/지진/홍수), 본 시스템이 **공간연산 또는 매핑테이블을 통해 읍면동까지 역산**하여 저장한다.

### 3.2 재난유형별 표준 속성

#### 3.2.1 태풍 (TYPHOON)

| 필드 | 타입 | 설명 |
|---|---|---|
| typhoonYear / typhoonNo | int | 발생연도 / 태풍번호 |
| typhoonNameKo / En | varchar | 태풍명 |
| forecastBaseTime | timestamptz | 예보 발표시각 |
| forecastSeq | int | 예보 시퀀스(0=현재, 이후 예상경로) |
| forecastTime | timestamptz | 해당 시점 |
| lat / lon | numeric(9,6) | 중심 위경도 |
| centralPressure | int | 중심기압(hPa) |
| maxWindSpeed | numeric(5,1) | 최대풍속(m/s) |
| radius15ms / radius25ms | int | 강풍/폭풍 반경(km) |
| moveDirection / moveSpeed | varchar / int | 진행방향 / 속도(km/h) |
| intensity / size | varchar | 강도(중/강/매우강/초강력) / 크기 |
| trackGeom | geometry(LineString,4326) | 경로 라인 (PostGIS) |
| affectedRegions | 매핑 | 반경 내 포함 읍면동 목록 |

#### 3.2.2 폭염 (HEATWAVE)

| 필드 | 설명 |
|---|---|
| alertLevel | `ADVISORY`(주의보) / `WARNING`(경보) |
| issuedAt / effectiveFrom / effectiveTo | 발표·시작·해제 시각 |
| maxTemp / apparentTemp | 최고기온 / 체감온도(℃) |
| regionCodes | 특보구역 → 시군구/읍면동 전개 |

#### 3.2.3 홍수 (FLOOD)

| 필드 | 설명 |
|---|---|
| stationCode / stationName | 수위관측소 코드/명 |
| riverName | 하천명 |
| waterLevel | 현재수위(m) |
| attentionLevel / warningLevel / alertLevel / seriousLevel | 관심/주의보/경보/심각 기준수위 |
| floodStage | `NORMAL`/`ATTENTION`/`WARNING`/`ALERT`/`SERIOUS` |
| flowRate | 유량(㎥/s) |
| observedAt | 관측시각 |

#### 3.2.4 산사태 (LANDSLIDE)

| 필드 | 설명 |
|---|---|
| riskLevel | `NONE`/`ATTENTION`/`WARNING`(주의보)/`ALERT`(경보) |
| soilWaterIndex | 토양함수지수(%) |
| rainfall1h / rainfall24h / rainfallCumulative | 강우량(mm) |
| regionCode | 읍면동 법정동코드 |
| issuedAt / releasedAt | 발령/해제 시각 |

#### 3.2.5 황사 (YELLOWDUST)

| 필드 | 설명 |
|---|---|
| alertLevel | `ADVISORY`/`WARNING` |
| pm10Value / pm10Hour24 | PM10 농도(㎍/㎥) / 24시간 이동평균 |
| grade | `GOOD`/`NORMAL`/`BAD`/`VERY_BAD` |
| stationCode | 측정소 코드 |

#### 3.2.6 낙뢰 (LIGHTNING)

| 필드 | 설명 |
|---|---|
| strikeTime | 낙뢰 발생시각 (ms 단위) |
| lat / lon | 낙뢰 지점 위경도 |
| strikeType | `CG`(대지방전) / `IC`(운내방전) |
| intensity | 전류값(kA), 극성 포함(±) |
| regionCode | 공간매핑된 읍면동 코드 |

#### 3.2.7 지진 (EARTHQUAKE)

| 필드 | 설명 |
|---|---|
| eventId | 지진 통보 고유번호 |
| originTime | 발생시각 |
| lat / lon / depth | 진앙 위경도 / 깊이(km) |
| magnitude | 규모(M) |
| maxIntensity | 최대 계기진도(MMI, I~XII) |
| epicenterDesc | 진앙 위치 설명 |
| tsunamiYn | 지진해일 여부 |
| regionIntensities | 시도/시군구별 계기진도 배열 |

---

## 4. 행정구역 체계

### 4.1 코드 규칙

법정동코드 10자리 기준. 계층은 코드 prefix로 결정한다.

| 레벨 | 코드 자리수 | 예시 | 설명 |
|---|---|---|---|
| `SIDO` (1단계) | 2 (+`00000000`) | `1100000000` | 서울특별시 |
| `SIGUNGU` (2단계) | 5 (+`00000`) | `1168000000` | 서울특별시 강남구 |
| `EUPMYEONDONG` (3단계) | 8 (+`00`) | `1168010100` | 서울특별시 강남구 역삼동 |

### 4.2 행정구역 테이블 요구사항

- **폐지/변경 이력 관리**: `valid_from`, `valid_to`, `use_yn` 컬럼으로 시점 조회 지원 (통폐합 대응)
- **계층 조회**: `parent_code` 자기참조 + `full_name`(시도 시군구 읍면동) 비정규화 컬럼
- **공간정보**: `boundary geometry(MultiPolygon,4326)` (읍면동 경계), `center_point geometry(Point,4326)`
- **격자 매핑**: 읍면동 ↔ 기상청 격자(nx, ny) 매핑 테이블 (다대다 허용, 대표격자 플래그)
- **특보구역 매핑**: 기상청 특보구역코드 ↔ 시군구/읍면동 매핑 테이블

### 4.3 지점 → 행정구역 역매핑 규칙

| 원천 데이터 형태 | 매핑 전략 |
|---|---|
| 점(Point) — 낙뢰, 지진 진앙 | `ST_Contains(boundary, point)` 로 읍면동 직접 판정. 미포함(해상) 시 `ST_Distance` 최근접 읍면동 + `offshore_yn=true` |
| 원(Center+Radius) — 태풍 | `ST_DWithin(boundary, center, radius)` 로 교차 읍면동 전량 추출, 반경등급별 저장 |
| 지점(Station) — 홍수, 황사 | 관측소 마스터에 사전 매핑된 읍면동 코드 사용 (관측소별 영향 읍면동 다대다 매핑 테이블) |
| 특보구역 — 폭염, 황사 | 특보구역 ↔ 행정구역 매핑 테이블 전개 |
| 읍면동 직접 제공 — 산사태 | 코드 정합성 검증 후 그대로 사용 |

> PostGIS 미도입 시 대안: 읍면동 경계 대신 **중심좌표 + Haversine 거리** 기반 근사 매핑을 사용하되, 태풍 영향권 판정 정확도가 낮아지므로 **PostGIS 도입을 권고**한다.

---

## 5. 연계 방식

### 5.1 주기 연계 (Scheduled Pull)

- 연계 정의(`if_definition`) 테이블의 `cron_expression` 을 읽어 동적 스케줄 등록
- 다중 인스턴스 환경에서 **ShedLock** 으로 단일 실행 보장
- 정의 변경 시 무중단 재등록 (`ScheduledTaskRegistrar` 재구성 또는 30초 주기 refresh)

**기본 연계 주기(권고값, 운영 중 조정 가능)**

| 재난유형 | 평시 주기 | 비상시 주기(경보 발효 중) | 비고 |
|---|---|---|---|
| 태풍 | 3시간 | 1시간 | 예보 발표주기 연동 |
| 폭염 | 1시간 | 10분 | 특보 발표 시 단축 |
| 홍수 | 10분 | 5분 | 수위 실시간성 요구 |
| 산사태 | 1시간 | 10분 | 강우 연동 |
| 황사 | 1시간 | 30분 | |
| 낙뢰 | 5분 | 1분 | 데이터량 최대 |
| 지진 | 1분 | 30초 | 최우선 실시간성 |

- **증분 수집**: 각 정의별 `last_success_data_time` 을 워터마크로 사용해 delta 조회. 원천이 delta를 지원하지 않으면 전량 조회 후 자연키 기준 **UPSERT**(`INSERT ... ON CONFLICT DO UPDATE`).
- **중복 방지**: 재난유형별 자연키(예: 태풍 = `year+no+baseTime+seq`, 낙뢰 = `strikeTime+lat+lon`)에 UNIQUE 제약.

### 5.2 실시간 연계 (On-Demand Pull)

- 소비 시스템이 `POST /api/v1/interfaces/{code}/trigger` 호출 시 즉시 원천 수집 수행
- **동기 모드**(`async=false`, 기본): 수집 완료까지 대기, 타임아웃 30초. 초과 시 `202 Accepted` + 실행ID 반환으로 전환
- **비동기 모드**(`async=true`): 즉시 실행ID 반환, 상태는 `GET /executions/{id}` 로 폴링
- **중복 실행 억제**: 동일 정의에 대해 실행 중인 건이 있으면 신규 실행을 생성하지 않고 진행 중 실행ID를 반환 (`duplicated=true`)
- **레이트리밋**: 정의별 최소 실행 간격(`min_interval_sec`, 기본 30초). 위반 시 `429` + `retryAfter` 반환
- **원천 보호**: Resilience4j RateLimiter 로 원천 기관별 초당 호출 수 제한

### 5.3 조회 API (Query)

수집·표준화된 데이터를 행정구역 단위로 제공. 상세는 §8 참조.

---

## 6. 재시도 및 장애 처리

### 6.1 재시도 정책 (필수 요구사항 R-5)

```
[실행 시작]
   │
   ├─▶ 1차 시도 ── 실패 ─▶ 즉시 재시도 대기(백오프) ─┐
   │                                                │
   ├─▶ 2차 시도 ── 실패 ─────────────────────────────┤
   │                                                │
   ├─▶ 3차 시도 ── 실패 ─▶ [즉시 재시도 3회 소진]    │
   │                              │                 │
   │                              ▼                 │
   │                    상태: RETRY_SCHEDULED        │
   │                    next_retry_at = now + 10분   │
   │                              │                 │
   │                              ▼                 │
   │                 (10분 후 스케줄러가 재실행) ────┘
   │                              │
   │                    ┌─────────┴──────────┐
   │                    ▼                    ▼
   │              성공 → SUCCESS      실패 → 재시도 사이클 반복
   │                                        (최대 지연 재시도 횟수 도달 시 FAILED + 알림)
   └─▶ 성공 → SUCCESS
```

**상세 규칙**

| 항목 | 값 | 설명 |
|---|---|---|
| 즉시 재시도 횟수 | **3회** | 1회 최초 시도 후 총 3회까지 시도 (`maxAttempts=3`) |
| 즉시 재시도 간격 | 2초 → 4초 (Exponential, multiplier 2.0, jitter ±20%) | Resilience4j Retry |
| 3회 초과 시 | **10분 후 재시도** | `next_retry_at = now() + interval '10 minutes'`, 상태 `RETRY_SCHEDULED` |
| 지연 재시도 최대 횟수 | 6회 (기본, 정의별 설정 가능) | 총 60분 후 `FAILED` 확정 |
| 지연 재시도 스케줄러 | 1분 주기 | `next_retry_at <= now()` 인 실행 건을 폴링하여 재실행 |
| 최종 실패 처리 | 상태 `FAILED` + `if_error_log` 적재 + 운영 알림(Webhook/메일) | |

**재시도 대상 판정**

| 오류 유형 | 재시도 | 비고 |
|---|---|---|
| Connect/Read Timeout, `5xx`, `429` | O | 일시적 장애 |
| DNS 실패, Connection Reset | O | |
| `401`, `403` (인증키 오류) | X | 즉시 `FAILED`, 운영 알림. 재시도해도 무의미 |
| `400` (요청 파라미터 오류) | X | 정의 수정 필요 |
| `404` | X | |
| 응답 파싱 실패 / 스키마 불일치 | X | 원천 규격 변경 의심, 운영 알림 |
| 데이터 무결성 위반(제약조건) | X | 정규화 로직 결함 |
| 원천 "데이터 없음" 정상 응답 | 해당없음 | `SUCCESS_NO_DATA` 로 종료 (실패 아님) |

### 6.2 서킷 브레이커

- 원천 기관 단위(`source_system`)로 CircuitBreaker 구성
- 최근 20건 중 실패율 50% 초과 시 `OPEN` → 5분 후 `HALF_OPEN` (시험 호출 3건)
- `OPEN` 상태에서 신규 실행은 즉시 `SKIPPED_CIRCUIT_OPEN` 으로 기록 (원천 부하 방지)

### 6.3 타임아웃

| 구분 | 값 |
|---|---|
| Connect Timeout | 5초 |
| Read Timeout | 30초 (낙뢰/대용량은 60초, 정의별 설정) |
| 전체 실행 타임아웃 | 5분 (초과 시 강제 중단 + 실패 처리) |

---

## 7. 데이터베이스 설계

### 7.1 스키마 구성

| 스키마 | 용도 |
|---|---|
| `common` | 공통코드, 행정구역, 격자, 관측소 마스터 |
| `disaster` | 재난 표준 데이터 7종 |
| `itf` | 연계 정의, 실행 이력, 로그 |

### 7.2 주요 DDL

#### 7.2.1 행정구역

```sql
CREATE TABLE common.region (
    region_code     CHAR(10)     PRIMARY KEY,           -- 법정동코드 10자리
    region_level    VARCHAR(20)  NOT NULL,              -- SIDO / SIGUNGU / EUPMYEONDONG
    parent_code     CHAR(10)     REFERENCES common.region(region_code),
    sido_code       CHAR(10)     NOT NULL,
    sigungu_code    CHAR(10),
    region_name     VARCHAR(100) NOT NULL,              -- 역삼동
    full_name       VARCHAR(300) NOT NULL,              -- 서울특별시 강남구 역삼동
    center_point    geometry(Point, 4326),
    boundary        geometry(MultiPolygon, 4326),
    valid_from      DATE         NOT NULL DEFAULT '1900-01-01',
    valid_to        DATE         NOT NULL DEFAULT '9999-12-31',
    use_yn          BOOLEAN      NOT NULL DEFAULT TRUE,
    created_at      TIMESTAMPTZ  NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ  NOT NULL DEFAULT now()
);
CREATE INDEX idx_region_level      ON common.region(region_level, use_yn);
CREATE INDEX idx_region_parent     ON common.region(parent_code);
CREATE INDEX idx_region_sigungu    ON common.region(sigungu_code);
CREATE INDEX idx_region_boundary   ON common.region USING GIST(boundary);
CREATE INDEX idx_region_center     ON common.region USING GIST(center_point);
CREATE INDEX idx_region_name_trgm  ON common.region USING GIN(full_name gin_trgm_ops);
```

```sql
-- 읍면동 ↔ 기상청 격자
CREATE TABLE common.region_grid (
    region_code CHAR(10) NOT NULL REFERENCES common.region(region_code),
    nx          SMALLINT NOT NULL,
    ny          SMALLINT NOT NULL,
    primary_yn  BOOLEAN  NOT NULL DEFAULT FALSE,
    PRIMARY KEY (region_code, nx, ny)
);

-- 기상특보구역 ↔ 행정구역
CREATE TABLE common.region_alert_zone (
    zone_code   VARCHAR(20) NOT NULL,
    zone_name   VARCHAR(100),
    region_code CHAR(10)    NOT NULL REFERENCES common.region(region_code),
    PRIMARY KEY (zone_code, region_code)
);

-- 관측소(수위/대기질) 마스터 및 영향 행정구역
CREATE TABLE common.station (
    station_code  VARCHAR(30) PRIMARY KEY,
    station_type  VARCHAR(20) NOT NULL,     -- WATER_LEVEL / AIR_QUALITY / AWS
    station_name  VARCHAR(100) NOT NULL,
    lat           NUMERIC(9,6),
    lon           NUMERIC(9,6),
    location      geometry(Point, 4326),
    region_code   CHAR(10) REFERENCES common.region(region_code),
    use_yn        BOOLEAN NOT NULL DEFAULT TRUE
);
CREATE TABLE common.station_region (
    station_code VARCHAR(30) NOT NULL REFERENCES common.station(station_code),
    region_code  CHAR(10)    NOT NULL REFERENCES common.region(region_code),
    PRIMARY KEY (station_code, region_code)
);
```

#### 7.2.2 연계 정의

```sql
CREATE TABLE itf.if_definition (
    if_id               BIGSERIAL    PRIMARY KEY,
    if_code             VARCHAR(50)  NOT NULL UNIQUE,   -- KMA_TYPHOON_FCST
    if_name             VARCHAR(200) NOT NULL,
    disaster_type       VARCHAR(20)  NOT NULL,          -- TYPHOON / HEATWAVE / ...
    source_system       VARCHAR(50)  NOT NULL,          -- KMA / HRFCO / FOREST / AIRKOREA
    endpoint_url        TEXT         NOT NULL,
    http_method         VARCHAR(10)  NOT NULL DEFAULT 'GET',
    request_params      JSONB        NOT NULL DEFAULT '{}'::jsonb,
    auth_type           VARCHAR(20)  NOT NULL DEFAULT 'API_KEY',
    auth_key_ref        VARCHAR(100),                   -- 시크릿 저장소 참조 키 (평문 저장 금지)
    response_format     VARCHAR(10)  NOT NULL DEFAULT 'JSON',
    cron_expression     VARCHAR(50),                    -- 주기 연계용, NULL이면 실시간 전용
    emergency_cron      VARCHAR(50),                    -- 경보 발효 중 단축 주기
    connect_timeout_ms  INT          NOT NULL DEFAULT 5000,
    read_timeout_ms     INT          NOT NULL DEFAULT 30000,
    max_attempts        SMALLINT     NOT NULL DEFAULT 3,       -- 즉시 재시도 3회
    retry_delay_minutes SMALLINT     NOT NULL DEFAULT 10,      -- 소진 후 10분 지연
    max_delayed_retries SMALLINT     NOT NULL DEFAULT 6,
    min_interval_sec    INT          NOT NULL DEFAULT 30,      -- 실시간 호출 최소 간격
    realtime_yn         BOOLEAN      NOT NULL DEFAULT TRUE,
    use_yn              BOOLEAN      NOT NULL DEFAULT TRUE,
    last_success_at         TIMESTAMPTZ,
    last_success_data_time  TIMESTAMPTZ,                       -- 증분 수집 워터마크
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
```

#### 7.2.3 연계 실행 이력 (월 파티션)

```sql
CREATE TABLE itf.if_execution (
    exec_id         BIGSERIAL,
    if_id           BIGINT       NOT NULL,
    if_code         VARCHAR(50)  NOT NULL,
    trigger_type    VARCHAR(20)  NOT NULL,   -- SCHEDULED / REALTIME / RETRY / MANUAL
    trigger_by      VARCHAR(100),            -- 요청자(시스템ID/사용자ID)
    status          VARCHAR(30)  NOT NULL,   -- RUNNING / SUCCESS / SUCCESS_NO_DATA
                                             -- / RETRY_SCHEDULED / FAILED / SKIPPED_CIRCUIT_OPEN / TIMEOUT
    attempt_no      SMALLINT     NOT NULL DEFAULT 1,   -- 즉시 시도 회차 (1~3)
    retry_round     SMALLINT     NOT NULL DEFAULT 0,   -- 10분 지연 재시도 회차 (0~6)
    parent_exec_id  BIGINT,                            -- 재시도 원본 실행
    next_retry_at   TIMESTAMPTZ,                       -- 지연 재시도 예정시각
    started_at      TIMESTAMPTZ  NOT NULL DEFAULT now(),
    finished_at     TIMESTAMPTZ,
    duration_ms     INT,
    request_url     TEXT,
    request_params  JSONB,
    http_status     SMALLINT,
    response_size   INT,
    total_count     INT DEFAULT 0,    -- 원천 수신 건수
    insert_count    INT DEFAULT 0,
    update_count    INT DEFAULT 0,
    skip_count      INT DEFAULT 0,
    error_count     INT DEFAULT 0,
    error_code      VARCHAR(30),
    error_message   TEXT,
    server_id       VARCHAR(50),      -- 실행 인스턴스 식별자
    trace_id        VARCHAR(50),      -- 분산추적 ID
    PRIMARY KEY (exec_id, started_at)
) PARTITION BY RANGE (started_at);

CREATE INDEX idx_exec_ifcode_time ON itf.if_execution(if_code, started_at DESC);
CREATE INDEX idx_exec_status      ON itf.if_execution(status, started_at DESC);
CREATE INDEX idx_exec_retry       ON itf.if_execution(next_retry_at)
    WHERE status = 'RETRY_SCHEDULED';
CREATE INDEX idx_exec_trace       ON itf.if_execution(trace_id);
```

#### 7.2.4 연계 상세 로그 / 오류 로그 (월 파티션)

```sql
-- 요청/응답 원문 및 단계별 로그
CREATE TABLE itf.if_log (
    log_id        BIGSERIAL,
    exec_id       BIGINT       NOT NULL,
    if_code       VARCHAR(50)  NOT NULL,
    log_level     VARCHAR(10)  NOT NULL,   -- DEBUG / INFO / WARN / ERROR
    log_phase     VARCHAR(20)  NOT NULL,   -- REQUEST / RESPONSE / PARSE / NORMALIZE
                                           -- / REGION_MAP / PERSIST / RETRY
    message       TEXT,
    request_body  TEXT,                    -- 마스킹 후 저장
    response_body TEXT,                    -- 최대 1MB, 초과 시 truncate + 원문은 오브젝트 스토리지
    elapsed_ms    INT,
    logged_at     TIMESTAMPTZ  NOT NULL DEFAULT now(),
    PRIMARY KEY (log_id, logged_at)
) PARTITION BY RANGE (logged_at);

CREATE INDEX idx_iflog_exec  ON itf.if_log(exec_id);
CREATE INDEX idx_iflog_time  ON itf.if_log USING BRIN(logged_at);

-- 오류 상세 (알림/통계용, 조회 빈도 높음)
CREATE TABLE itf.if_error_log (
    error_id      BIGSERIAL,
    exec_id       BIGINT       NOT NULL,
    if_code       VARCHAR(50)  NOT NULL,
    disaster_type VARCHAR(20),
    error_code    VARCHAR(30)  NOT NULL,   -- §10 에러코드
    error_type    VARCHAR(30)  NOT NULL,   -- CONNECTION / TIMEOUT / AUTH / PARSE
                                           -- / VALIDATION / PERSIST / UNKNOWN
    retryable_yn  BOOLEAN      NOT NULL,
    attempt_no    SMALLINT,
    retry_round   SMALLINT,
    error_message TEXT,
    stack_trace   TEXT,
    notified_yn   BOOLEAN      NOT NULL DEFAULT FALSE,
    occurred_at   TIMESTAMPTZ  NOT NULL DEFAULT now(),
    PRIMARY KEY (error_id, occurred_at)
) PARTITION BY RANGE (occurred_at);
```

#### 7.2.5 재난 데이터 예시 (태풍 · 낙뢰)

```sql
CREATE TABLE disaster.typhoon_track (
    track_id        BIGSERIAL PRIMARY KEY,
    typhoon_year    SMALLINT     NOT NULL,
    typhoon_no      SMALLINT     NOT NULL,
    typhoon_name_ko VARCHAR(50),
    typhoon_name_en VARCHAR(50),
    base_time       TIMESTAMPTZ  NOT NULL,
    forecast_seq    SMALLINT     NOT NULL,   -- 0=현재위치, 1..n=예상경로
    forecast_time   TIMESTAMPTZ  NOT NULL,
    lat             NUMERIC(9,6) NOT NULL,
    lon             NUMERIC(9,6) NOT NULL,
    location        geometry(Point, 4326),
    central_pressure INT,
    max_wind_speed   NUMERIC(5,1),
    radius_15ms      INT,
    radius_25ms      INT,
    move_direction   VARCHAR(10),
    move_speed       INT,
    intensity        VARCHAR(20),
    typhoon_size     VARCHAR(20),
    exec_id          BIGINT,
    created_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT uk_typhoon_track
        UNIQUE (typhoon_year, typhoon_no, base_time, forecast_seq)
);

-- 태풍 영향 행정구역 (반경 교차 결과)
CREATE TABLE disaster.typhoon_region (
    track_id     BIGINT   NOT NULL REFERENCES disaster.typhoon_track(track_id) ON DELETE CASCADE,
    region_code  CHAR(10) NOT NULL,
    impact_level VARCHAR(20) NOT NULL,   -- STRONG_WIND(15m/s) / STORM(25m/s)
    PRIMARY KEY (track_id, region_code, impact_level)
);
```

```sql
CREATE TABLE disaster.lightning (
    lightning_id BIGSERIAL,
    strike_time  TIMESTAMPTZ  NOT NULL,
    lat          NUMERIC(9,6) NOT NULL,
    lon          NUMERIC(9,6) NOT NULL,
    location     geometry(Point, 4326),
    strike_type  VARCHAR(5),              -- CG / IC
    intensity_ka NUMERIC(7,2),            -- 극성 포함
    region_code  CHAR(10),                -- 공간매핑된 읍면동
    sigungu_code CHAR(10),
    sido_code    CHAR(10),
    offshore_yn  BOOLEAN NOT NULL DEFAULT FALSE,
    exec_id      BIGINT,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (lightning_id, strike_time),
    CONSTRAINT uk_lightning UNIQUE (strike_time, lat, lon)
) PARTITION BY RANGE (strike_time);

CREATE INDEX idx_lightning_region ON disaster.lightning(region_code, strike_time DESC);
CREATE INDEX idx_lightning_time   ON disaster.lightning USING BRIN(strike_time);
```

> 나머지 5종(폭염/홍수/산사태/황사/지진) 테이블도 동일 원칙으로 설계한다: `region_code`/`sigungu_code`/`sido_code` 3단계 컬럼 비정규화, 자연키 UNIQUE, `exec_id` 추적 컬럼, 대용량 테이블은 월 RANGE 파티션.

### 7.3 파티션 및 보존 정책

| 테이블 | 파티션 | 보존기간 | 아카이브 |
|---|---|---|---|
| `itf.if_execution` | 월 | 12개월 | 이후 DETACH + 압축 보관 |
| `itf.if_log` | 월 | 3개월 | 이후 삭제 (오브젝트 스토리지 이관) |
| `itf.if_error_log` | 월 | 12개월 | |
| `disaster.lightning` | 월 | 24개월 | |
| 기타 재난 데이터 | 월/미적용 | 5년 | 법정 보존 요건에 따름 |

- 파티션 자동 생성: `pg_partman` 확장 또는 매일 01:00 배치로 향후 2개월분 사전 생성
- 오래된 파티션은 `DETACH PARTITION` 후 `DROP` (LOCK 최소화)

### 7.4 PostgreSQL 17 활용 포인트

- **파티션 pruning** 및 파티션 단위 `MERGE`/`SPLIT PARTITION` 활용
- `INSERT ... ON CONFLICT DO UPDATE` 기반 UPSERT (배치 1,000건 단위)
- `MERGE ... RETURNING` 으로 insert/update 건수 정확 집계
- JSONB + `jsonb_path_ops` GIN 인덱스로 원천 원문 보관 및 조회
- 로그 시계열 컬럼은 **BRIN 인덱스**로 인덱스 크기 최소화
- `COPY` (CopyManager) 로 낙뢰 등 대량 적재 성능 확보

---

## 8. API 명세

### 8.1 공통 규격

- Base URL: `https://{host}/api/v1`
- 인증: `Authorization: Bearer {JWT}` 또는 `X-API-KEY` (시스템 간 연계)
- 시각: 모두 ISO-8601 + KST 오프셋 (`2026-08-14T13:20:00+09:00`)
- 페이징: `page`(0-base), `size`(기본 20, 최대 1000), `sort`

**공통 응답**

```json
{
  "success": true,
  "code": "S000",
  "message": "정상 처리되었습니다.",
  "data": { },
  "timestamp": "2026-08-14T13:20:00+09:00",
  "traceId": "a1b2c3d4e5"
}
```

### 8.2 재난정보 조회 API

| Method | Path | 설명 |
|---|---|---|
| GET | `/disasters` | 전체 재난정보 통합 조회 (유형/지역/기간 필터) |
| GET | `/disasters/typhoons` | 태풍 목록 |
| GET | `/disasters/typhoons/{year}/{no}/track` | 태풍 경로(현재+예상) |
| GET | `/disasters/heatwaves` | 폭염 특보 |
| GET | `/disasters/floods` | 홍수/수위 |
| GET | `/disasters/landslides` | 산사태 위험 |
| GET | `/disasters/yellow-dusts` | 황사 |
| GET | `/disasters/lightnings` | 낙뢰 |
| GET | `/disasters/earthquakes` | 지진 |
| GET | `/disasters/summary` | 지역별 현재 재난 상황 요약 |

**공통 쿼리 파라미터**

| 파라미터 | 타입 | 설명 |
|---|---|---|
| `regionCode` | string | 법정동코드. **prefix 매칭** — `11`(시도), `11680`(시군구), `1168010100`(읍면동) 모두 허용 |
| `regionLevel` | enum | 응답 집계 단위: `SIDO` / `SIGUNGU` / `EUPMYEONDONG` (기본: 요청 코드 레벨) |
| `from` / `to` | datetime | 조회 기간 (기본 최근 24시간, 최대 31일) |
| `disasterTypes` | string[] | `/disasters` 통합조회 시 유형 필터 |
| `activeOnly` | boolean | 현재 발효 중인 건만 (기본 false) |

**예시**

```http
GET /api/v1/disasters/summary?regionCode=11680&regionLevel=EUPMYEONDONG&activeOnly=true
```

```json
{
  "success": true,
  "code": "S000",
  "data": {
    "region": {
      "regionCode": "1168000000",
      "regionLevel": "SIGUNGU",
      "fullName": "서울특별시 강남구"
    },
    "baseTime": "2026-08-14T13:00:00+09:00",
    "items": [
      {
        "regionCode": "1168010100",
        "fullName": "서울특별시 강남구 역삼동",
        "disasters": [
          {
            "disasterType": "HEATWAVE",
            "alertLevel": "WARNING",
            "issuedAt": "2026-08-14T11:00:00+09:00",
            "detail": { "maxTemp": 37.2, "apparentTemp": 39.5 }
          },
          {
            "disasterType": "LIGHTNING",
            "alertLevel": null,
            "detail": { "strikeCount1h": 12, "lastStrikeTime": "2026-08-14T12:47:33+09:00" }
          }
        ]
      }
    ],
    "page": { "number": 0, "size": 20, "totalElements": 22, "totalPages": 2 }
  }
}
```

### 8.3 실시간 연계 요청 API

```http
POST /api/v1/interfaces/{ifCode}/trigger
Content-Type: application/json

{
  "async": false,
  "params": { "regionCode": "1168000000" },
  "force": false
}
```

| 필드 | 설명 |
|---|---|
| `async` | true 시 즉시 실행ID 반환 (기본 false) |
| `params` | 연계 정의 기본 파라미터에 병합할 추가 파라미터 |
| `force` | true 시 `min_interval_sec` 무시 (관리자 권한 필요) |

**응답 (동기 성공)**

```json
{
  "success": true,
  "code": "S000",
  "data": {
    "execId": 98213,
    "ifCode": "KMA_TYPHOON_FCST",
    "status": "SUCCESS",
    "startedAt": "2026-08-14T13:20:01+09:00",
    "finishedAt": "2026-08-14T13:20:04+09:00",
    "durationMs": 3120,
    "totalCount": 42, "insertCount": 8, "updateCount": 34, "errorCount": 0,
    "duplicated": false
  }
}
```

**응답 (실패 → 지연 재시도 예약, HTTP 200 + 상태값으로 표현)**

```json
{
  "success": false,
  "code": "E201",
  "message": "원천 시스템 응답 시간 초과. 3회 재시도 후 10분 뒤 재시도 예정입니다.",
  "data": {
    "execId": 98214,
    "status": "RETRY_SCHEDULED",
    "attemptNo": 3,
    "retryRound": 0,
    "nextRetryAt": "2026-08-14T13:30:04+09:00"
  }
}
```

| Method | Path | 설명 |
|---|---|---|
| POST | `/interfaces/{ifCode}/trigger` | 실시간 연계 실행 |
| POST | `/interfaces/trigger-batch` | 복수 정의 동시 실행 |
| GET | `/interfaces/executions/{execId}` | 실행 상태 조회 |
| POST | `/interfaces/executions/{execId}/cancel` | 실행 중단 / 예약 재시도 취소 |

### 8.4 운영·관리 API

| Method | Path | 설명 |
|---|---|---|
| GET | `/admin/interfaces` | 연계 정의 목록 |
| POST/PUT | `/admin/interfaces`, `/admin/interfaces/{ifCode}` | 정의 등록/수정 |
| PATCH | `/admin/interfaces/{ifCode}/status` | 사용여부(use_yn) 토글 |
| GET | `/admin/executions` | 실행 이력 조회 (ifCode/status/기간/triggerType 필터) |
| GET | `/admin/executions/{execId}/logs` | 실행별 상세 로그 |
| GET | `/admin/errors` | 오류 로그 조회 (errorCode/errorType/기간) |
| GET | `/admin/statistics/daily` | 일자별 성공/실패/평균 소요시간 통계 |
| POST | `/admin/executions/{execId}/retry` | 수동 즉시 재시도 |
| GET | `/admin/health/interfaces` | 정의별 최종 성공시각·지연 여부 |
| GET | `/admin/circuit-breakers` | 원천별 서킷 상태 |

### 8.5 행정구역 API

| Method | Path | 설명 |
|---|---|---|
| GET | `/regions?level=SIDO` | 시도 목록 |
| GET | `/regions/{regionCode}/children` | 하위 행정구역 (시도→시군구→읍면동) |
| GET | `/regions/search?keyword=역삼` | 명칭 검색 |
| GET | `/regions/reverse?lat=&lon=` | 좌표 → 행정구역 역조회 |

---

## 9. 연계 로그 관리 (요구사항 R-6)

### 9.1 로그 계층

| 계층 | 저장소 | 내용 | 보존 |
|---|---|---|---|
| 실행 이력 (`if_execution`) | PostgreSQL | 실행 단위 요약: 상태, 건수, 소요시간, 재시도 회차 | 12개월 |
| 상세 로그 (`if_log`) | PostgreSQL | 단계별(REQUEST/RESPONSE/PARSE/…) 원문 및 경과시간 | 3개월 |
| 오류 로그 (`if_error_log`) | PostgreSQL | 오류코드/유형/스택트레이스/알림여부 | 12개월 |
| 애플리케이션 로그 | 파일 → ELK/Loki | Logback JSON, `traceId` 로 DB 로그와 조인 | 1개월 |

### 9.2 로그 적재 규칙

- 실행 이력은 **REQUIRES_NEW 트랜잭션**으로 분리 적재 → 본 처리 롤백 시에도 이력 보존
- 상세 로그는 **비동기 큐(BlockingQueue) + 배치 flush(1초 또는 200건)** 로 적재하여 본 처리 지연 방지
- `response_body` 는 1MB 초과 시 앞 1MB만 저장하고 `truncated=true` 표시
- **마스킹 필수**: `serviceKey`, `authKey`, `apiKey`, `Authorization` 헤더, 개인정보성 필드 → `****` 치환 후 저장
- 모든 로그는 `traceId`(MDC) 를 포함하여 애플리케이션 로그와 상호 추적 가능

### 9.3 모니터링 지표 (Micrometer)

| 지표명 | 타입 | 설명 |
|---|---|---|
| `itf.execution.count{ifCode,status}` | Counter | 실행 건수 |
| `itf.execution.duration{ifCode}` | Timer | 실행 소요시간 |
| `itf.retry.count{ifCode,type}` | Counter | 즉시/지연 재시도 횟수 |
| `itf.records.processed{ifCode,op}` | Counter | insert/update/skip 건수 |
| `itf.source.latency{sourceSystem}` | Timer | 원천 응답시간 |
| `itf.circuit.state{sourceSystem}` | Gauge | 서킷 상태 |
| `itf.staleness.seconds{ifCode}` | Gauge | 마지막 성공 이후 경과시간 |

### 9.4 알림 규칙

| 조건 | 등급 | 채널 |
|---|---|---|
| 지연 재시도 진입 (즉시 3회 실패) | WARN | Slack/Webhook |
| 최종 `FAILED` 확정 | ERROR | Slack + 메일 |
| 인증 오류(`401/403`) | CRITICAL | Slack + 메일 (즉시) |
| 서킷 `OPEN` 전환 | ERROR | Slack |
| `staleness` > 주기 × 3 | WARN | Slack |
| 지진 연계 실패 | CRITICAL | 즉시 (재난 특성상 최우선) |

---

## 10. 에러 코드

| 코드 | HTTP | 재시도 | 설명 |
|---|---|---|---|
| `S000` | 200 | - | 정상 처리 |
| `S001` | 200 | - | 정상 처리(수집 데이터 없음) |
| `E100` | 400 | X | 필수 파라미터 누락 |
| `E101` | 400 | X | 잘못된 행정구역 코드 |
| `E102` | 400 | X | 조회 기간 초과(최대 31일) |
| `E110` | 401 | X | 인증 실패 |
| `E111` | 403 | X | 권한 없음 |
| `E120` | 404 | X | 연계 정의 없음 |
| `E121` | 404 | X | 실행 이력 없음 |
| `E130` | 409 | X | 이미 실행 중인 연계 (진행 실행ID 반환) |
| `E131` | 429 | X | 최소 실행 간격 미충족 |
| `E200` | 502 | O | 원천 시스템 연결 실패 |
| `E201` | 504 | O | 원천 시스템 응답 시간 초과 |
| `E202` | 502 | O | 원천 시스템 오류 응답(5xx) |
| `E203` | 502 | X | 원천 인증키 오류(401/403) |
| `E204` | 502 | X | 원천 응답 파싱 실패 |
| `E205` | 503 | X | 서킷 브레이커 OPEN |
| `E300` | 500 | X | 데이터 정규화 실패 |
| `E301` | 500 | X | 행정구역 매핑 실패 |
| `E302` | 500 | O | DB 적재 실패 |
| `E900` | 500 | X | 알 수 없는 오류 |

---

## 11. 비기능 요구사항

### 11.1 성능

| 항목 | 목표 |
|---|---|
| 조회 API 응답시간 | P95 < 300ms, P99 < 800ms (읍면동 단위 24시간 조회) |
| 실시간 연계 API | P95 < 5s (원천 응답시간 포함) |
| 낙뢰 대량 적재 | 10,000건 / 5초 이내 (COPY 기반) |
| 동시 처리 | 조회 API 200 TPS, 연계 실행 동시 20건 |
| 스케줄 정확도 | 지정 시각 ± 10초 |

### 11.2 가용성 / 확장성

- 애플리케이션 2대 이상 이중화, ShedLock 으로 스케줄 중복 방지
- 무중단 배포(Rolling), 실행 중 작업은 Graceful Shutdown 60초 대기
- DB: Primary + Standby(스트리밍 복제), 조회 API 는 Standby 라우팅 검토
- RTO 30분, RPO 5분

### 11.3 보안

- 원천 API 키는 **DB 평문 저장 금지** — 환경변수/Vault 등 시크릿 저장소 참조(`auth_key_ref`)
- 모든 외부 통신 HTTPS(TLS 1.2+)
- API 인증: 시스템 간 `X-API-KEY` + IP 화이트리스트, 사용자 `JWT`
- 관리 API 는 `ROLE_ADMIN` 권한 필수
- 로그 마스킹(§9.2), 감사 로그(정의 변경/수동 재시도 이력) 적재

### 11.4 데이터 품질

| 검증 항목 | 처리 |
|---|---|
| 위경도 범위 (한반도: lat 33~39, lon 124~132) | 벗어나면 `WARN` 로그 + 저장 (태풍은 해상 포함이므로 범위 확대) |
| 필수 필드 NULL | 해당 레코드 `skip` + 오류 로그 |
| 미래 시각 데이터 (예보 제외) | `skip` |
| 미등록 행정구역 코드 | `region_code = NULL` 저장 + `E301` 경고 로그 |
| 중복 자연키 | UPSERT 처리 (update_count 집계) |

---

## 12. 개발 계획

| 단계 | 기간 | 산출물 |
|---|---|---|
| 1. 기반 구축 | 2주 | 프로젝트 셋업, Flyway 스키마, 행정구역/격자 데이터 적재, 공통 응답/예외 |
| 2. 연계 엔진 | 3주 | 연계 정의 CRUD, Executor, RetryManager(3회+10분), 로그 적재, 스케줄러(ShedLock) |
| 3. 어댑터 개발 I | 3주 | 지진, 폭염, 황사, 산사태 (구조 단순 4종) |
| 4. 어댑터 개발 II | 3주 | 태풍(경로/PostGIS), 낙뢰(대량적재), 홍수(관측소) |
| 5. 조회 API | 2주 | 재난 조회 API, 행정구역 API, Swagger |
| 6. 운영 기능 | 2주 | 관리 API, 모니터링 지표, 알림 연동, 파티션 자동화 |
| 7. 테스트/안정화 | 2주 | 통합·부하 테스트, 장애 시나리오(재시도/서킷) 검증, 운영 문서 |

**총 17주 (약 4개월)**

### 12.1 완료 조건 (Definition of Done)

- [ ] 7종 재난정보가 시도/시군구/읍면동 단위로 조회 가능
- [ ] 재난유형별 주기 연계가 스케줄대로 동작하며, 다중 인스턴스에서 중복 실행되지 않음
- [ ] 실시간 연계 API 로 즉시 수집 가능 (동기/비동기 모두)
- [ ] 원천 장애 주입 시 즉시 3회 재시도 → 10분 후 재시도 → 최종 실패 흐름이 이력에 정확히 기록됨
- [ ] 모든 실행이 `if_execution` 에 남고, 인증키 등 민감정보가 마스킹되어 저장됨
- [ ] 단위 테스트 커버리지 80% 이상, 어댑터별 원천 응답 Mock 테스트 존재

---

## 13. 리스크 및 대응

| 리스크 | 영향 | 대응 |
|---|---|---|
| 원천 API 규격 변경 | 연계 전면 중단 | 응답 스키마 검증 + 파싱 실패 시 즉시 알림, 원문(JSONB) 보관으로 사후 재처리 |
| 원천 기관별 호출 쿼터 초과 | 차단 | RateLimiter, 실시간 API 최소 간격 제한, 캐시 활용 |
| 낙뢰 데이터 폭증 (뇌우 시) | DB 부하 | 파티셔닝 + COPY 적재 + BRIN 인덱스, 필요 시 별도 커넥션 풀 |
| 행정구역 통폐합 | 매핑 오류 | `valid_from/to` 시점 관리, 분기별 코드 동기화 배치 |
| PostGIS 미도입 | 태풍/낙뢰 매핑 정확도 저하 | 도입 권고. 미도입 시 중심좌표+거리 근사 및 정확도 한계 명시 |
| 지진 등 초긴급 정보 지연 | 서비스 신뢰도 | 지진 전용 최단주기 스케줄 + 실패 시 CRITICAL 즉시 알림 |

---

## 부록 A. 재난유형 코드

| 코드 | 명칭 | 영문 |
|---|---|---|
| `TYPHOON` | 태풍 | Typhoon |
| `HEATWAVE` | 폭염 | Heat Wave |
| `FLOOD` | 홍수 | Flood |
| `LANDSLIDE` | 산사태 | Landslide |
| `YELLOWDUST` | 황사 | Yellow Dust |
| `LIGHTNING` | 낙뢰 | Lightning |
| `EARTHQUAKE` | 지진 | Earthquake |

## 부록 B. 실행 상태 코드

| 코드 | 설명 |
|---|---|
| `RUNNING` | 실행 중 |
| `SUCCESS` | 성공 |
| `SUCCESS_NO_DATA` | 성공(수집 데이터 없음) |
| `RETRY_SCHEDULED` | 즉시 재시도 3회 소진, 10분 후 재시도 예약 |
| `FAILED` | 최종 실패 |
| `TIMEOUT` | 전체 실행 타임아웃 |
| `SKIPPED_CIRCUIT_OPEN` | 서킷 OPEN 으로 실행 생략 |
| `CANCELLED` | 수동 취소 |
