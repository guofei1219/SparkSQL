#渠道进件客户数
INSERT OVERWRITE TABLE rpt_asset_qlty_chnl_totl partition(dt='2016-11-14',type=13)
select
 t.chnl_code                                                    -- 渠道代码
,13 as ind_type                                                 -- 营业部代码
,count(t.id_num) as intpc_cust_cnt                              -- 营业部进件客户数
,regexp_replace(substr(t.intpc_crt_dt,1,7),'-','') as bill_mth  -- 账单月
,current_timestamp      as    etl_dt                            -- 批量日期
from (   select row_number() over (partition by chnl_code,upper(id_num) order by chnl_code,upper(id_num),intpc_crt_dt asc)as  num,
              chnl_code,id_num,intpc_crt_dt
       from  blb_intpc_info
       where dt='2016-11-22' and end_dt='9999-12-31' and
             UPPER(substr(chnl_code,1,5))='306DC'
     ) t where t.num = 1
group by  t.chnl_code , regexp_replace(substr(t.intpc_crt_dt,1,7),'-','')

