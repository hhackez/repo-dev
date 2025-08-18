# 1. 베이스 이미지 설정
# 공식 파이썬 3.10 이미지를 기반으로 합니다. 'slim' 버전은 용량이 작습니다.
FROM python:3.10-slim

# 2. 작업 디렉터리 설정
# 컨테이너 내에서 명령어가 실행될 기본 경로를 설정합니다.
WORKDIR /app

# 3. 라이브러리 설치
# requirements.txt 파일을 먼저 복사하여 라이브러리를 설치합니다.
# 이렇게 하면 앱 코드만 변경되었을 때 Docker 빌드 캐시를 활용하여 빌드 속도를 높일 수 있습니다.
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# 4. 앱 소스 코드 복사
# 현재 디렉터리의 모든 파일을 컨테이너의 작업 디렉터리로 복사합니다.
COPY . .

# 5. 포트 노출
# Streamlit의 기본 포트인 8501을 외부로 노출합니다.
EXPOSE 8501

# 6. 앱 실행 명령어
# 컨테이너가 시작될 때 실행할 명령어를 정의합니다.
# Streamlit을 컨테이너 외부에서 접속할 수 있도록 주소와 포트를 설정합니다.
CMD ["streamlit", "run", "main.py", "--server.port=8502", "--server.address=0.0.0.0"]
