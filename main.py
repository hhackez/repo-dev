import streamlit as st
import pandas as pd
# from trino.dbapi import connect # SQLAlchemy 사용으로 더 이상 필요하지 않음
from sqlalchemy import create_engine # SQLAlchemy 엔진 생성을 위해 import
from datetime import datetime
import pyarrow
import s3fs # S3 파일 시스템을 위해 import
import os # 시간대 설정을 위해 os 모듈 추가

# --- 시간대 설정 ---
# 서버의 시간대 충돌 오류를 해결하기 위해 스크립트의 시간대를 명시적으로 설정합니다.
os.environ['TZ'] = 'Asia/Seoul'

# --- 페이지 레이아웃 설정 ---
# st.set_page_config는 스크립트에서 가장 먼저 실행되어야 하는 Streamlit 명령어입니다.
# layout="wide"로 설정하여 페이지를 넓은 모드로 사용합니다.
st.set_page_config(layout="wide")


# --- Trino DB 연결 및 쿼리 함수 ---
@st.cache_data
def query_trino(track_id: int):
    """
    Trino 데이터베이스에 연결하여 주어진 track_id에 대한 데이터를 조회합니다.
    결과는 Pandas DataFrame으로 반환합니다.
    """
    try:
        # st.secrets에서 접속 정보 불러오기
        host = st.secrets.trino.host
        port = st.secrets.trino.port
        user = st.secrets.trino.user
        catalog = "hive"
        schema = "flo_reco_dev"

        # SQLAlchemy를 사용하여 Trino 엔진 생성
        engine = create_engine(
            f"trino://{user}@{host}:{port}/{catalog}/{schema}"
        )
        
        query = f"""
        SELECT *
        FROM flo_reco_dev.tb_vd_track_nsp06_stat
        WHERE track_id_src = {track_id}
        """
        # pd.read_sql에 SQLAlchemy 엔진을 전달하여 경고(Warning)를 해결합니다.
        df = pd.read_sql(query, engine)
        return df
    except Exception as e:
        st.error(f"데이터베이스 연결 또는 쿼리 실행 중 오류가 발생했습니다: {e}")
        return pd.DataFrame()


# --- Streamlit UI 구성 ---
st.title("🎵 가사 탐지 Demo")
st.write("Trino DB와 연동하여 Track ID를 검색하는 예제입니다.")

if 'feedback_form_visible' not in st.session_state:
    st.session_state.feedback_form_visible = False

user_number = st.number_input("Track ID를 입력하세요:", min_value=0, step=1, value=None, placeholder="예: 12345")

if st.button("DB에서 검색", type="primary"):
    st.session_state.feedback_form_visible = False
    if user_number is not None and user_number > 0:
        with st.spinner('Trino DB에서 데이터를 검색 중입니다...'):
            st.session_state.result_df = query_trino(user_number)
            st.session_state.searched_id = user_number
    else:
        st.info("검색할 Track ID를 입력해주세요.")
        if 'result_df' in st.session_state:
            del st.session_state.result_df
        if 'searched_id' in st.session_state:
            del st.session_state.searched_id

if 'result_df' in st.session_state:
    result_df = st.session_state.result_df

    if not result_df.empty:
        st.success(f"Track ID `{st.session_state.searched_id}`에 대한 데이터를 찾았습니다!")
        st.dataframe(result_df)

        try:
            # 링크 생성을 위해 사용하는 컬럼명을 'track_id'에서 'track_id_src'로 변경합니다.
            # 이 컬럼명은 DB 쿼리 결과에 실제 존재하는 이름이어야 합니다.
            track_id_value = result_df['track_id_src'].iloc[0]

            # track_id_value가 유효한 값인지 확인 (None이나 0이 아닌 경우)
            if pd.notna(track_id_value) and track_id_value:
                track_id_for_url = str(track_id_value)
                link_url = f"https://www.music-flo.com/detail/track/{track_id_for_url}/details"
                st.markdown(f"🔗 **[1분 듣기 실행]({link_url})**", unsafe_allow_html=True)
            else:
                st.error("링크를 생성할 수 있는 유효한 Track ID가 없습니다.")

        except KeyError:
            # 만약 'track_id_src'도 아니라면, 실제 컬럼명을 확인해야 합니다.
            st.error("결과 데이터에서 'track_id_src' 컬럼을 찾을 수 없습니다. 컬럼명을 확인해주세요.")
        except IndexError:
            st.error("결과 데이터가 비어있어 링크를 생성할 수 없습니다.")

        st.divider()

        if st.button("피드백 입력"):
            st.session_state.feedback_form_visible = not st.session_state.feedback_form_visible

        if st.session_state.feedback_form_visible:
            with st.form("feedback_form"):
                st.subheader("피드백 남기기")
                author = st.text_input("작성자")
                feedback_content = st.text_area("피드백 내용")
                
                submitted = st.form_submit_button("제출")
                if submitted:
                    if author and feedback_content:
                        # --- S3에 Parquet 파일로 저장하는 로직 ---
                        try:
                            # 1. 저장할 데이터프레임 생성
                            creation_timestamp = datetime.now()
                            feedback_data = {
                                "author": [author],
                                "feedback_content": [feedback_content],
                                "creation_timestamp": [creation_timestamp]
                            }
                            feedback_df = pd.DataFrame(feedback_data)

                            # 2. S3 경로 및 고유한 파일 이름 정의
                            s3_path = "s3://flo-reco-dev/database/flo_reco_dev/tb_vd_track_feedback/"
                            file_name = f"feedback_{creation_timestamp.strftime('%Y%m%d_%H%M%S_%f')}.parquet"
                            full_s3_path = f"{s3_path}{file_name}"

                            # 3. st.secrets를 사용하여 S3에 파일 저장
                            storage_options = {
                                "key": st.secrets.aws.aws_access_key_id,
                                "secret": st.secrets.aws.aws_secret_access_key,
                            }
                            feedback_df.to_parquet(full_s3_path, engine='pyarrow', storage_options=storage_options)
                            
                            st.success("제출 완료")
                            st.session_state.feedback_form_visible = False
                            st.rerun() # 폼을 즉시 숨기기 위해 페이지를 새로고침

                        except Exception as e:
                            st.error(f"S3에 파일을 저장하는 중 오류가 발생했습니다: {e}")

                    else:
                        st.warning("작성자와 피드백 내용을 모두 입력해주세요.")
    else:
        st.warning(f"Track ID `{st.session_state.searched_id}`에 해당하는 데이터를 찾을 수 없습니다.")
