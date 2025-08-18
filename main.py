import streamlit as st
import pandas as pd
from sqlalchemy import create_engine
import os

# --- 기본 설정 ---
# 시간대 설정
os.environ['TZ'] = 'Asia/Seoul'
# 페이지 레이아웃을 넓게 설정
st.set_page_config(layout="wide")


# --- 데이터베이스 연결 및 쿼리 함수 ---
@st.cache_data
def query_single_track(track_id: int):
    """지정된 단일 Track ID에 대한 모든 정보를 조회합니다."""
    try:
        engine = create_engine(
            f"trino://{st.secrets.trino.user}@{st.secrets.trino.host}:{st.secrets.trino.port}/hive/flo_reco_dev"
        )
        query = f"SELECT * FROM flo_reco_dev.tb_vd_track_nsp06_stat WHERE track_id_src = {track_id}"
        df = pd.read_sql(query, engine)
        return df
    except Exception as e:
        st.error(f"단일 트랙 조회 중 오류가 발생했습니다: {e}")
        return pd.DataFrame()

@st.cache_data
def fetch_track_list(limit: int, offset: int):
    """조건에 맞는 트랙 목록을 지정된 수만큼 가져옵니다."""
    try:
        engine = create_engine(
            f"trino://{st.secrets.trino.user}@{st.secrets.trino.host}:{st.secrets.trino.port}/hive/flo_reco_dev"
        )
        # [수정] track_all 테이블을 JOIN하고 pop_score를 SELECT에 추가하며, pop_score로 정렬하도록 쿼리 수정
        # NULLS LAST를 추가하여 pop_score가 없는 곡은 목록의 뒤로 보냅니다.
        query = f"""
        SELECT
            nsp.track_id_src,
            nsp.track_title,
            nsp.nsp06_unique_word_cnt,
            ta.pop_score
        FROM flo_reco_dev.tb_vd_track_nsp06_stat AS nsp
        LEFT JOIN flo_reco_dev.track_all AS ta
            ON nsp.track_id_src = ta.track_id
        WHERE
            nsp.lyric_yn = 'N' AND nsp.nsp06_unique_word_cnt >= 10
        ORDER BY
            ta.pop_score DESC NULLS LAST
        OFFSET {offset}
        LIMIT {limit}
        """
        df = pd.read_sql(query, engine)
        return df
    except Exception as e:
        st.error(f"트랙 목록 조회 중 오류가 발생했습니다: {e}")
        return pd.DataFrame()


# --- Streamlit UI 구성 ---
st.title("🎵 가사 탐지 Demo")

# --- 1. 단일 Track ID 검색 섹션 ---
st.header("1. 단일 Track ID 검색")
user_number = st.number_input("Track ID를 입력하세요:", min_value=0, step=1, value=None, placeholder="예: 12345", key="single_track_input")

if st.button("DB에서 검색", type="primary"):
    if user_number is not None and user_number > 0:
        with st.spinner('데이터를 검색 중입니다...'):
            st.session_state.single_search_result = query_single_track(user_number)
            st.session_state.single_searched_id = user_number
    else:
        st.info("검색할 Track ID를 입력해주세요.")
        if 'single_search_result' in st.session_state:
            del st.session_state.single_search_result

# 단일 검색 결과 표시
if 'single_search_result' in st.session_state:
    result_df = st.session_state.single_search_result
    if not result_df.empty:
        st.success(f"Track ID `{st.session_state.single_searched_id}`에 대한 데이터를 찾았습니다!")
        st.dataframe(result_df)
        try:
            track_id_value = result_df['track_id_src'].iloc[0]
            if pd.notna(track_id_value) and track_id_value:
                link_url = f"https://www.music-flo.com/detail/track/{str(track_id_value)}/details"
                st.markdown(f"🔗 **[1분 듣기 실행]({link_url})**", unsafe_allow_html=True)
        except (KeyError, IndexError):
            st.error("링크를 생성할 수 있는 Track ID를 찾지 못했습니다.")
    else:
        st.warning(f"Track ID `{st.session_state.single_searched_id}`에 해당하는 데이터를 찾을 수 없습니다.")


st.divider()


# --- 2. 가사 미등록 트랙 목록 조회 섹션 ---
st.header("2. 가사 미등록 트랙 목록")

# 목록 조회를 위한 세션 상태 초기화
if 'track_list_data' not in st.session_state:
    st.session_state.track_list_data = pd.DataFrame()
    st.session_state.list_page = 0

# 목록 불러오기 버튼
if st.button("목록 불러오기"):
    st.session_state.track_list_data = pd.DataFrame()
    st.session_state.list_page = 0
    with st.spinner('데이터를 불러오는 중입니다...'):
        initial_data = fetch_track_list(limit=20, offset=0)
        st.session_state.track_list_data = initial_data
        st.session_state.list_page = 1

# 목록 데이터가 있을 경우에만 표와 '더 보기' 버튼 표시
if not st.session_state.track_list_data.empty:
    st.dataframe(st.session_state.track_list_data)
    
    if st.button("더 보기"):
        with st.spinner('추가 데이터를 불러오는 중입니다...'):
            offset = st.session_state.list_page * 20
            more_data = fetch_track_list(limit=20, offset=offset)
            
            if not more_data.empty:
                st.session_state.track_list_data = pd.concat(
                    [st.session_state.track_list_data, more_data],
                    ignore_index=True
                )
                st.session_state.list_page += 1
                st.rerun()
            else:
                st.info("더 이상 가져올 데이터가 없습니다.")
else:
    st.info("'목록 불러오기' 버튼을 클릭하여 데이터를 확인하세요.")
