# .cursor (WebSocket)

이 폴더는 **websocket** (Pathfinder WebSocket 서버) 프로젝트에서 Cursor·AI가 참고할 필수 안내를 둡니다.

## AI 필수 안내

- **이 문서와 `rules/` 규칙은 websocket 폴더에서 작업하는 AI가 반드시 읽고 따라야 합니다.**
- **코드 수정 후에는 반드시 관련 문서를 최신화하세요.**
  - 예: 프로토콜·포트·설정·CLI 옵션 변경 시 루트 `README.md`, 주석, 상위 저장소(pathfinder-containers)의 관련 문서(예: FEATURES_SINCE_*.md, docker-compose 관련 설명) 갱신
  - 관련 README, 주석, 설정 설명이 실제 동작과 맞는지 확인하고 수정

| 파일 | 설명 |
|------|------|
| **README.md** (본 문서) | .cursor 개요 및 AI 필수 안내 |
| **../README.md** | WebSocket 서버 설치·설정·시작 방법 |
| **rules/** | Cursor 규칙 — 코드 수정 시 문서 최신화 등 |
