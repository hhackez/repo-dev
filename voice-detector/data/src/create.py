import os
import boto3
import pandas as pd
from base import Base

class CreateSource(Base):
    """
    Spark SQL을 사용하여 여러 테이블을 조인하고,
    특정 조건에 맞는 track_id를 조회하여 S3 단일 CSV 파일로 저장하는 클래스.
    """
    def run(self):
        print(">> 'create' 작업을 시작합니다: 데이터 조회 및 파일 생성")

        # 1️⃣ config에서 테이블명 가져오기
        track_all_table = self.config['input']['track_all']
        mcp_track_table = self.config['input']['mcp_track']
        vd_track_table = self.config['input']['vd_track']
        
        # --- [수정] config에서 제외할 style_id 목록 가져오기 ---
        exclude_style_ids = self.config["filter"]["exclude_style_ids"]
        print(f">> 적용할 필터: {len(exclude_style_ids)}개의 style ID 제외")
        
        # Spark SQL의 array() 함수에 사용할 문자열 생성
        # 예: [70203, 70204] -> "array(70203, 70204)"
        exclude_ids_str = ", ".join(map(str, exclude_style_ids))
        sql_exclude_array = f"array({exclude_ids_str})"
        # --- [수정 완료] ---

        # 2️⃣ SQL 쿼리 정의
        # [수정] 
        #  - 조인 키를 'vdt.track_id'로 변경
        #  - WHERE절을 'vdt.track_id IS NULL'로 변경
        #  - [추가] style_id_list 필터링 조건 추가
        query = f"""
            SELECT rct.track_id
            FROM {track_all_table} rct
            INNER JOIN {mcp_track_table} mt
                ON rct.track_id = mt.b_track_id
            LEFT OUTER JOIN {vd_track_table} vdt
                ON rct.track_id = vdt.track_id
            WHERE rct.pop_score >= 0.2
              AND mt.lyric_yn = 'N'
              AND mt.sync_lyric_yn = 'N'
              AND vdt.track_id IS NULL
              AND (rct.style_id_list IS NULL OR NOT arrays_overlap(rct.style_id_list, {sql_exclude_array}))
        """
        print(">> 실행 쿼리:\n", query)

        # 3️⃣ Spark SQL 실행 후 캐시
        result_df = self.spark.sql(query)
        result_df.cache()
        
        count = result_df.count()
        print(f">> 조회된 track 개수: {count}")

        if count == 0:
            print(">> 조회된 신규 track이 없으므로 S3 파일 생성을 건너뜁니다.")
            result_df.unpersist()
            return

        # 4️⃣ 로컬 임시 CSV 파일 경로
        local_tmp_dir = "/tmp"
        file_name = self.config['output']['file_name']
        local_tmp_file = os.path.join(local_tmp_dir, f"{file_name}")

        print(f">> 임시 로컬 CSV 파일로 저장: {local_tmp_file}")
        # [수정] .toPandas()는 데이터가 적을 때만 사용해야 합니다.
        #      현재 로직은 데이터가 적다고 가정합니다.
        result_df.toPandas().to_csv(local_tmp_file, index=False, header=True)

        # 5️⃣ S3 업로드
        # Base에서 self.footprint는 이미 task 경로 포함
        s3_footprint = self.footprint.rstrip("/")  # e.g., s3://flo-reco-dev/footprint/voice_detector/create
        s3_bucket = s3_footprint.split("/")[2]     # flo-reco-dev
        s3_prefix = "/".join(s3_footprint.split("/")[3:])  # footprint/voice_detector/create
        s3_key = f"{s3_prefix}/{file_name}"

        # --- [추가] S3 업로드 경로 변수 값 출력 ---
        print("\n>> S3 업로드 경로 디버깅:")
        print(f"   s3_footprint: {s3_footprint}")
        print(f"   s3_bucket   : {s3_bucket}")
        print(f"   s3_prefix   : {s3_prefix}")
        print(f"   s3_key      : {s3_key}")
        print(f"   local_file  : {local_tmp_file}\n")
        # --- [추가 완료] ---

        s3_client = boto3.client("s3")
        s3_client.upload_file(local_tmp_file, s3_bucket, s3_key)

        print(f">> ✅ 단일 CSV S3 업로드 완료: s3://{s3_bucket}/{s3_key}")

        # 6️⃣ 캐시 해제 및 임시 파일 삭제
        result_df.unpersist()
        try:
            os.remove(local_tmp_file)
            print(f">> 로컬 임시 파일 삭제: {local_tmp_file}")
        except OSError as e:
            print(f"[WARNING] 로컬 임시 파일 삭제 실패: {e}")

