CREATE TABLE flo_reco_dev.tb_vd_track (
    track_id_src BIGINT, 
    text_length BIGINT, 
    all_seg_cnt BIGINT, 
    total_duration DOUBLE,
    speech_duration DOUBLE, 
    speech_ratio DOUBLE, 
    nsp06_seg_cnt BIGINT,
    nsp06_word_cnt BIGINT, 
    nsp06_unique_word_cnt BIGINT
)
USING csv
OPTIONS (
    path 's3a://flo-reco-dev/database/flo_reco_dev/tb_vd_track/',
    header 'true',
    delimiter ','
);


--[spark sql]
CREATE TABLE flo_reco_dev.tb_vd_track_nsp06_stat AS
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
FROM flo_reco_dev.tb_vd_track_meta vd
LEFT JOIN flo_reco_dev.tnmm_track mt
    ON vd.track_id = mt.b_track_id
left join (
select track_id
    , concat_ws(',', collect_list(artist_text)) AS artist_array
from flo_mcp.tnmm_track_artist 
where artist_role_type = 'VOCAL'
group by track_id
) ta
    on mt.track_id = ta.track_id
LEFT JOIN (
select track_id
    , concat_ws(',', collect_list(svc_genre_nm)) AS genre_array
from (
select sgt.track_id
    , sgt.svc_genre_id 
    , sg.svc_genre_nm
from flo_poc.tb_map_svc_genre_track sgt
inner join flo_poc.tb_svc_genre sg
    on sgt.svc_genre_id = sg.svc_genre_id
)
group by track_id
) tg
    on vd.track_id = tg.track_id
;



select rct.track_id 
	, rct.track_nm 
	, mt.track_artist_name_list
	, mt.lyric_yn
	, mt.sync_lyric_yn 
	, tg.genre_array
--	, tg.genre_id_array
	, rct.pop_score 
from flo_reco.track_all rct 
inner join flo_mcp.tnmm_track mt   --95169
	on rct.track_id = mt.b_track_id
inner join ( --25499
	SELECT track_id,
	       LISTAGG(svc_genre_nm, ',') AS genre_array,
	       LISTAGG(svc_genre_id, ',') AS genre_id_array
	FROM (
	    SELECT sgt.track_id,
	           sgt.svc_genre_id,
	           sg.svc_genre_nm
	    FROM flo_poc.tb_map_svc_genre_track sgt
	    INNER JOIN flo_poc.tb_svc_genre sg
	        ON sgt.svc_genre_id = sg.svc_genre_id
		) t
	GROUP BY track_id 
	) tg 
	on rct.track_id = tg.track_id
where rct.pop_score >= 0.2  --0.5:31446, 0.6:10766
and mt.lyric_yn = 'N'
and mt.sync_lyric_yn = 'N'
and (tg.genre_id_array ilike '%19%' or tg.genre_id_array ilike '%20%' or tg.genre_id_array ilike '%22%')  --19:OST/BGM 20:클래식 22:재즈
order by 7 desc 
;