import os
import argparse
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, lit, arrays_overlap

from base import Base
from util import get_yyyyymmdd


class Merger(Base):
    """
    Source 테이블의 데이터를 필터링하여 Target 테이블로 적재하고,
    최종 통계 테이블을 생성하는 클래스입니다.
    """

    def __init__(self, task: str, config_path: str, yyyymmdd: str) -> None:
        super().__init__(task=task, config_path=config_path, yyyymmdd=yyyymmdd)
        # 설정 파일에서 필터링에서 제외할 스타일 ID 목록을 읽어옵니다.
        self.exclude_style_ids = self.config["filter"]["exclude_style_ids"]
        print(f">> Exclude Style IDs: {self.exclude_style_ids}")

    def run(self) -> None:
        """
        데이터 병합, 필터링, 최종 테이블 생성 프로세스를 실행합니다.
        작업 1: Source 테이블 데이터를 Target 테이블에 삽입합니다.
        작업 2: Target 테이블에서 필터 조건에 맞는 데이터를 제외하고 다시 생성합니다.
        작업 3: Target 테이블과 MCP/POC 테이블을 조인하여 최종 통계 테이블을 생성합니다.
        """
        print(f"[{self.yyyymmdd}] Start merging process...")

        # 1. 설정 파일에서 테이블 정보 가져오기
        source_table = self.config["input"]["source_table"]
        target_table = self.config["output"]["target_table"]
        temp_table = self.config["output"]["temp_table"]
        # filter_table = self.config["filter"]["filter_table"]
        # [수정] Task 2에서 사용할 테이블 정보 로드
        style_filter_table = self.config["filter"]["filter_table"]
        lyric_filter_table = self.config["output"]["filter_table"] # config.merge.dev.yml 참조

        # --- 작업 1: source -> target으로 insert 수행 ---
        print(">> Task 1: Inserting data from source to target...")
        try:
            source_df = self.spark.table(source_table)
            print(f"source_df row count = {source_df.count()}")
            # 데이터 파티셔닝을 위해 처리 날짜 컬럼 추가
            # if "dt" in source_df.columns:
            #     source_df = source_df.drop("dt")
            # source_df_with_dt = source_df.withColumn("dt", lit(self.yyyymmdd))

            # Target 테이블에 데이터 추가 (Append)
            source_df_with_dt_casted = source_df.withColumn(
                "track_id", col("track_id").cast("bigint")   # target_table의 타입에 맞춰 캐스팅
            )
            
            source_df_with_dt_casted.printSchema()
            
            source_df_with_dt_casted.write.mode("append").insertInto(target_table)
            print(f"Successfully inserted data from {source_table} to {target_table}.")
            
            # target_df = self.spark.table(target_table)
            # print(f"target_df row count = {target_df.count()}")
        except Exception as e:
            print(f"Error during Task 1 (inserting data): {e}")
            raise e

        # --- [수정] 작업 2: target에서 (1) style_id 필터 (2) lyric_yn 필터 적용하여 target 재생성 ---
        print("\n>> Task 2: Re-creating target table with filtered data...")
        try:
            # 2-1. 테이블 읽기
            print(f">> Reading updated target table: {target_table}")
            target_df_updated = self.spark.table(target_table)

            print(f">> Reading style filter table: {style_filter_table}")
            style_filter_df = self.spark.table(style_filter_table)
            
            print(f">> Reading lyric filter table: {lyric_filter_table}")
            lyric_filter_df = self.spark.table(lyric_filter_table).select("track_id", "lyric_yn")

            # 2-2. (1) 스타일 ID 필터링 로직 (제외할 track_id 목록 생성)
            print(">> Finding tracks to exclude based on style_id...")
            exclude_track_ids_df = style_filter_df.filter(
                arrays_overlap(col("style_id_list"), lit(self.exclude_style_ids))
            ).select("track_id").distinct()

            print(f">> Found {exclude_track_ids_df.count()} tracks to exclude based on style.")
            
            # 2-3. (1) target 테이블에서 style_id 제외 (left_anti join)
            style_filtered_df = target_df_updated.join(
                exclude_track_ids_df, on="track_id", how="left_anti"
            )
            print(f">> Rows after style_id filter: {style_filtered_df.count()}")

            # 2-4. (2) lyric_yn = 'N' 필터링 로직 (lyric_yn='N'인 track_id만 포함)
            print(">> Applying lyric_yn = 'N' filter...")
            # lyric_filter_df (tnmm_track)와 조인하여 lyric_yn 컬럼 추가
            joined_df = style_filtered_df.join(
                lyric_filter_df,
                on="track_id",
                how="left" # target_table에 있으나 tnmm_track에 없는 곡을 위해 left join
            ).fillna('N', subset=['lyric_yn']) # tnmm_track에 없으면 'N'으로 간주

            # lyric_yn = 'N' 인 곡만 필터링
            final_df = joined_df.filter(col("lyric_yn") == 'N')
            print(f">> Rows after lyric_yn filter: {final_df.count()}")

            print(">> Final DataFrame schema after filtering:")
            final_df.printSchema()

            # 2-5. 중복 제거
            dedup_df = (
                final_df
                .filter(col("track_id").isNotNull())   # track_id null 제거
                .dropDuplicates(["track_id"])          # track_id 기준 중복 제거
                .drop("lyric_yn") # 필터링에 사용한 lyric_yn 컬럼 제거
            )
            print(f">> Rows after deduplication: {dedup_df.count()}")
            
            # 2-6. 필터링된 데이터로 임시 테이블 덮어쓰기
            print(f">> Overwriting Temp table: {temp_table}")
            dedup_df.write.mode("overwrite").saveAsTable(temp_table)

            # 2-7. 임시 테이블의 데이터를 원본 테이블로 덮어쓰기
            print(f">> Overwriting original table ({target_table}) with data from temporary table.")
            temp_df = self.spark.table(temp_table)
            temp_df.write.mode("overwrite").saveAsTable(target_table)
            
            print(f"Successfully re-created target table: {target_table}.")
        except Exception as e:
            print(f"Error during Task 2 (filtering and recreating target): {e}")
            raise e
        # --- [수정 완료] ---

        # --- [추가] 작업 3: final_table 데이터 재 생성 ---
        print("\n>> Task 3: Re-creating final statistics table...")
        try:
            # 3-1. 설정 파일에서 final 테이블들 이름 가져오기
            final_config = self.config["final"]
            final_table = final_config["final_table"]
            mcp_track_table = final_config["mcp_track_table"]
            mcp_artist_table = final_config["mcp_artist_table"]
            poc_genre_map_table = final_config["poc_genre_map_table"]
            poc_genre_table = final_config["poc_genre_table"]
            
            # 3-2. final_table 데이터 모두 삭제 (TRUNCATE 사용)
            print(f">> Deleting all data from {final_table}...")
            self.spark.sql(f"TRUNCATE TABLE {final_table}")
            print(f"Successfully truncated {final_table}.")
            
            # 3-3. 쿼리 생성 (설정 파일의 테이블명 사용)
            print(f">> Generating insert query for {final_table}...")
            insert_query = f"""
            INSERT INTO {final_table}
            SELECT 
                CAST(vd.track_id AS bigint) AS track_id,
                CAST(mt.track_title AS varchar(100)) AS track_title,
                CAST(ta.artist_array as varchar(100))  as artist_array,
                CAST(tg.genre_array as varchar(100))  as genre_array,
                CAST(mt.lyric_yn AS varchar(2)) AS lyric_yn,
                CAST(vd.nsp06_word_cnt AS bigint) AS nsp06_word_cnt,
                CAST(vd.nsp06_unique_word_cnt AS bigint) AS nsp06_unique_word_cnt,
                CAST(CASE WHEN vd.nsp06_unique_word_cnt >= 28 THEN 'Y' ELSE 'N' END AS varchar(2)) AS nsp06uc28,
                CAST(CASE WHEN vd.nsp06_unique_word_cnt >= 20 THEN 'Y' ELSE 'N' END AS varchar(2)) AS nsp06uc20,
                CAST(CASE WHEN vd.nsp06_unique_word_cnt >= 16 THEN 'Y' ELSE 'N' END AS varchar(2)) AS nsp06uc16,
                CAST(CASE WHEN vd.nsp06_unique_word_cnt >= 13 THEN 'Y' ELSE 'N' END AS varchar(2)) AS nsp06uc13,
                CAST(CASE WHEN vd.nsp06_unique_word_cnt >= 10 THEN 'Y' ELSE 'N' END AS varchar(2)) AS nsp06uc10
            FROM {target_table} vd
            LEFT JOIN {mcp_track_table} mt
                ON vd.track_id = mt.b_track_id
            LEFT JOIN (
                SELECT track_id
                    , concat_ws(',', collect_list(artist_text)) AS artist_array
                FROM {mcp_artist_table}
                WHERE artist_role_type = 'VOCAL'
                GROUP BY track_id
            ) ta
                ON mt.track_id = ta.track_id
            LEFT JOIN (
                SELECT track_id
                    , concat_ws(',', collect_list(svc_genre_nm)) AS genre_array
                FROM (
                    SELECT sgt.track_id
                        , sgt.svc_genre_id 
                        , sg.svc_genre_nm
                    FROM {poc_genre_map_table} sgt
                    INNER JOIN {poc_genre_table} sg
                        ON sgt.svc_genre_id = sg.svc_genre_id
                )
                GROUP BY track_id
            ) tg
                ON vd.track_id = tg.track_id
            """
            
            # 3-4. Spark SQL로 쿼리 실행
            print(f">> Executing insert query into {final_table}...")
            self.spark.sql(insert_query)
            print(f"Successfully inserted new data into {final_table}.")

        except Exception as e:
            print(f"Error during Task 3 (re-creating final table): {e}")
            raise e
        # --- [추가 완료] ---

        print(f"[{self.yyyymmdd}] Merging process finished.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Merge and filter data and save to a Hive table."
    )
    parser.add_argument(
        "--yyyymmdd",
        type=str,
        default=get_yyyyymmdd(),
        help="The date partition to process, in YYYYMMDD format. Defaults to today.",
    )
    args = parser.parse_args()

    CONFIG_PATH = "res/config.merge.dev.yml"
    merger = None

    try:
        # Merger 인스턴스 생성 (누락되었던 'task' 인수 추가)
        merger = Merger(task="merge", config_path=CONFIG_PATH, yyyymmdd=args.yyyymmdd)
        # 데이터 병합 및 필터링 실행
        merger.run()
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
    finally:
        # SparkSession 종료
        if merger:
            merger.close()
