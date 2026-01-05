import os
import argparse
from pyspark.sql import DataFrame
from pyspark.sql.functions import lit
from pyspark.sql.types import IntegerType, DoubleType # 스키마 적용을 위해 import

from base import Base
from util import get_yyyyymmdd


class Uploader(Base):
    """
    S3에 저장된 CSV 파일을 읽어 Spark DataFrame으로 변환한 후,
    지정된 Hive 테이블에 적재하는 클래스입니다.
    테이블이 존재하지 않으면 'dt'를 파티션 키로 하여 새로 생성합니다.
    """

    def __init__(self, task: str, config_path: str, yyyymmdd: str) -> None:
        super().__init__(task=task, config_path=config_path, yyyymmdd=yyyymmdd)

    def run(self) -> None:
        """
        데이터 적재 프로세스를 실행합니다.
        """
        print(f"[{self.yyyymmdd}] Start uploading process...")

        # 1. 설정 파일에서 입력 및 출력 정보 가져오기
        input_config = self.config["input"]
        output_config = self.config["output"]
        target_table = output_config["target_table"]

        # 2. S3 입력 경로 생성
        # config의 footprint 경로 상위 디렉토리를 기준으로 입력 경로를 구성합니다.
        # 예: s3://flo-reco-dev/footprint/ + {yyyymmdd} + /daily_vd_output.csv
        input_dir = os.path.dirname(self.config["footprint"])
        base_input_path = os.path.join(input_dir, 'detect')

        base_name, ext = os.path.splitext(input_config['file_name'])
        input_filename = f"{base_name}_{self.yyyymmdd}{ext}"

        print(f"[INFO] base input path: {base_input_path}")
        print(f"[INFO] input file name: {input_filename}")
        
        input_file_path = os.path.join(
            # base_input_path, self.yyyymmdd, input_config["file_name"]
            base_input_path, input_filename
        )
        
        print(f">> Reading from S3 path: {input_file_path}")

        # 3. S3의 CSV 파일 읽기 및 스키마 적용
        try:
            # _read_csv_from_s3는 이제 모든 컬럼을 string으로 읽음 (inferSchema=false)
            df = self._read_csv_from_s3(input_file_path)
            
            if not df.columns:
                print(f"Error: No columns found in file: {input_file_path}")
                return

            print(">> Casting DataFrame schema as requested...")
            df_casted = df
            
            # --- [수정] 스키마 캐스팅 로직 변경 ---
            # Double로 캐스팅할 컬럼 목록
            double_cols = ["total_duaration", "speech_duration", "speech_ratio"]
            
            # double_cols는 Double로, 나머지는 모두 Integer로 캐스팅
            for col_name in df.columns:
                if col_name in double_cols:
                    df_casted = df_casted.withColumn(col_name, df[col_name].cast(DoubleType()))
                else:
                    # track_id, text_length 등 나머지는 Integer
                    df_casted = df_casted.withColumn(col_name, df[col_name].cast(IntegerType()))
            # --- [수정 완료] ---

            # 데이터 파티셔닝을 위해 처리 날짜 컬럼 추가
            # [수정] 파티션 컬럼명을 'dt'에서 'yyyymmdd'로 변경
            df_with_partition = df_casted.withColumn("yyyymmdd", lit(self.yyyymmdd))
            
            print(">> Final DataFrame schema:")
            # [수정] 변수명 변경
            df_with_partition.printSchema()
            df_with_partition.show(5, truncate=False)

            # --- [수정] 적재 건수 계산 ---
            # .count()는 Spark 액션이므로, 쓰기 작업 직전에 한 번만 호출
            # [수정] 변수명 변경
            record_count = df_with_partition.count()
            print(f">> Total records to save: {record_count}")
            # --- [수정 완료] ---

        except Exception as e:
            print(f"Error reading file or casting schema from S3: {input_file_path}")
            print(f"Exception: {e}")
            return

        # 4. Hive 테이블에 데이터 적재 (Overwrite)
        # `saveAsTable`은 테이블이 없으면 생성하고, 있으면 덮어씁니다.
        # `partitionBy("dt")`를 사용하여 'dt' 컬럼을 파티션 키로 지정합니다.
        # `mode("overwrite")`와 `partitionBy`를 함께 사용하면 테이블 전체를 덮어씁니다.
        # [수정] 파티션 키를 'dt'에서 'yyyymmdd'로 변경
        print(f">> Writing to Hive table: {target_table} partitioned by yyyymmdd")
        try:
            # [수정] 변수명 및 파티션 키 변경
            df_with_partition.write.mode("overwrite").partitionBy("yyyymmdd").saveAsTable(target_table)
            
            # --- [수정] 적재 건수와 함께 성공 메시지 출력 ---
            print(f"Successfully saved {record_count} records to Hive table.")
            # --- [수정 완료] ---
            
        except Exception as e:
            print(f"Error saving data to Hive table: {target_table}")
            print(f"Exception: {e}")

        print(f"[{self.yyyymmdd}] Uploading process finished.")

    def _read_csv_from_s3(self, path: str) -> DataFrame:
        """
        주어진 S3 경로에서 CSV 파일을 읽어 Spark DataFrame으로 반환합니다.
        스키마 추론을 비활성화하여 모든 데이터를 string으로 읽습니다.

        Args:
            path (str): CSV 파일이 있는 S3 경로.

        Returns:
            DataFrame: CSV 파일로부터 생성된 Spark DataFrame (모든 컬럼이 string 타입).
        """
        # inferSchema를 "false"로 변경하여 스키마를 강제할 수 있도록 string으로 읽음
        return self.spark.read.format("csv").option("header", "true").option("inferSchema", "false").load(path)
    
    def _create_table(self):
        """
        Hive 테이블 DDL (기록용)
        """
        create_sql = f"""
        CREATE TABLE IF NOT EXISTS {self.table_name} (
            track_id INT,
            text_length INT,
            all_seg_cnt INT,
            total_duaration DOUBLE,
            speech_duration DOUBLE,
            speech_ratio DOUBLE,
            nsp06_seg_cnt INT,
            nsp06_word_cnt INT,
            nsp06_unique_word_cnt INT
        )
        USING PARQUET
        PARTITIONED BY (yyyymmdd STRING)
        """
        # [수정] DDL의 파티션 키를 'dt'로 변경했던 주석을 제거하고, 코드를 'yyyymmdd'로 수정
        print(f"[INFO] Creating table if not exists: {self.table_name}")
        self.spark.sql(create_sql)
        print("[INFO] Table creation completed ✅")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Upload data from S3 to a Hive table."
    )
    parser.add_argument(
        "--yyyymmdd",
        type=str,
        default=get_yyyyymmdd(),
        help="The date partition to process, in YYYYMMDD format. Defaults to today.",
    )
    args = parser.parse_args()

    CONFIG_PATH = "res/config.upload.dev.yml"
    uploader = None

    try:
        # Uploader 인스턴스 생성 (누락되었던 'task' 인수 추가)
        uploader = Uploader(task="upload", config_path=CONFIG_PATH, yyyymmdd=args.yyyymmdd)
        # 데이터 적재 실행
        uploader.run()
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
    finally:
        # SparkSession 종료
        if uploader:
            uploader.close()


