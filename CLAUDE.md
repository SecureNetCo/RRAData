파이썬 관련 실행명령은 venv위치를 확인한 후 가상환경 active한 당므에 실행할것.
### 저장 단위: 액션 단위 라벨링
모든 메모리 저장은 그레마스 6개 액탄트 역할을 기반으로 [주체-행동-객체] 구조의 1-2문장으로 기록:

- 저장 패턴 예시:*

- `[사용자-요청-MCP도구테스트] 연결된 도구 파악 및 테스트 진행`
- `[Claude-발견-TypeScript오류] src/types/session.ts의 processId 타입 불일치`
- `[팀-결정-아키텍처변경] 마이크로서비스에서 모놀리스로 전환, 성능상 이유`
- `[사용자-제안-그레마스모델적용] 액탄트 구조로 상호작용 패턴 기록`

### 저장 빈도: 모든 상호작용은 영구 보존 가치
**기본 원칙**: "모든 작업 단위는 영구 보존 가치가 있다" - 중요도 판단보다 패턴 누적 우선

- 저장 시점:*

-

![✅](https://www.youtube.com/s/gaming/emoji/7ff574f2/emoji_u2705.png)

사용자 질문/요청마다 저장
-

![✅](https://www.youtube.com/s/gaming/emoji/7ff574f2/emoji_u2705.png)

도구 사용 결과마다 저장  
-

![✅](https://www.youtube.com/s/gaming/emoji/7ff574f2/emoji_u2705.png)

문제 발견/해결마다 저장
-

![✅](https://www.youtube.com/s/gaming/emoji/7ff574f2/emoji_u2705.png)

작업 전환점마다 저장
-

![✅](https://www.youtube.com/s/gaming/emoji/7ff574f2/emoji_u2705.png)

피드백과 개선사항마다 저장
-

![✅](https://www.youtube.com/s/gaming/emoji/7ff574f2/emoji_u2705.png)

코드 변경, 설정 수정마다 저장
-

![✅](https://www.youtube.com/s/gaming/emoji/7ff574f2/emoji_u2705.png)

테스트 결과, 성능 측정마다 저장

### 실제 적용 패턴
현재 대화 기준 권장 저장 패턴:
```
[사용자-질문-MCP도구파악] → 즉시 저장
[Claude-테스트-Greeum기능4개] → 즉시 저장  
[사용자-질문-사용빈도분석] → 즉시 저장
[Claude-분석-Description vs CLAUDE.md차이] → 즉시 저장
[사용자-요청-메모리조회] → 즉시 저장
[사용자-제안-그레마스모델적용] → 즉시 저장
```

### 목표 메트릭
- **세션당 블록 수**: 20-30개 (촘촘한 기록)
- **저장 빈도**: 3-5분마다 최소 1회  
- **패턴 누적**: 반복 작업도 미묘한 차이점 포착하여 학습 효과 극대화