#渠道放款客户数计算
INSERT OVERWRITE TABLE rpt_asset_qlty_chnl_totl partition(dt='2016-11-14',type=12)
select
t.chnl_code,
12 as ind_type,
count(t.id_num)                      as contr_cust_cnt,
regexp_replace(substring(t.loan_dt, 1 ,7),'-','')              as bill_mth      ,
current_timestamp      as    etl_dt                     -- 批量日期
from
(
    select row_number() over (partition by a.id_num order by a.intpc_crt_dt asc)as  num,
                 a.chnl_code,a.id_num,a.intpc_crt_dt,b.loan_dt
   from
      blb_intpc_info a
   inner join
      blm_contr_info c         -- 放款合同信息表
      on c.intpc_id = a.intpc_id and a.dt='2016-11-14' and  a.end_dt='9999-12-31' and UPPER(substr(a.chnl_code,1,5))='306DC'
      and c.dt='2016-11-14' and  c.end_dt='9999-12-31'
   inner join
   (
        select cust_card_id,contract_num,max(loan_dt) as loan_dt  from agg_asset_debit_info where dt='2016-10-31' and substr(bill_m,1,7)='2016-10' group by cust_card_id,contract_num
   ) b on b.cust_card_id = a.id_num and b.contract_num=c.contr_num and  c.dt='2016-11-14' and  c.end_dt='9999-12-31' and a.dt='2016-11-14' and  a.end_dt='9999-12-31'

)t where t.num=1
group by regexp_replace(substring(t.loan_dt, 1 ,7),'-',''),t.chnl_code
