  SELECT p.COID as Hospital_COID,
    nbt.COID as HBS_COID,
    nbt.sub_service_line_code as Facility_Type,
    lu.PE_Date,
    lu.Year_id,
    p.attending_md_npi,
    CAST(avg(ps.pat_sat_score) as FLOAT64) AS pat_sat_score,
    sum(p.encounter_los_amt) AS total_los,
    sum(p.drg_geometric_los_amt) AS total_geometric_los
    
  FROM
    -- --------------------------Patient Level Detail Summary-------------------------------------
    (
      SELECT
          cast(fpd.Patient_DW_ID as string) as Patient_DW_ID,
          fpd.pat_acct_num,
          fpd.coid,
          fpd.attending_md_npi,
          fpd.attending_md_name,
          fpd.discharge_date,
          max(CASE WHEN valesco.npi IS NOT NULL THEN 'Y'
             ELSE 'N'
             END) AS Valesco_Provider,
          fpd.encounter_los_amt,
          fpd.drg_geometric_los_amt,
          --fpd.encounter_los_amt/fpd.drg_geometric_los_amt as GMLOS
          
        FROM `hca-hin-prod-cur-clinical.edwcdm_pc_views.fact_ce_patient_detail_crnt`  AS fpd
        LEFT JOIN `hca-hin-prod-cur-ops.edwpf_views.fact_patient` fp
              ON FP.PATIENT_DW_ID = FPD.PATIENT_DW_ID

 -- -------------------------------------------Valesco Provider Flag------------------------------------------------------
        LEFT JOIN
    (
      SELECT DISTINCT
          pro.npi,
          comast.hospital_coid,
          ff_0.lob_code,
          concat(pro.last_name, ', ', pro.first_name) AS provider_name
        FROM
          `hca-hin-prod-cur-psg.edwps_efr_views.gl_coid_department_provider` AS nbt
          INNER JOIN `hca-hin-prod-cur-psg.edwps_efr_views.ref_coid_dept_relt` AS relt ON nbt.provider_relationship_name = relt.coid_dept_relt_desc
          INNER JOIN `hca-hin-prod-cur-psg.edwps_efr_views.gl_provider` AS pro ON pro.provider_src_sys_key = nbt.provider_src_sys_key
           AND pro.pe_date = nbt.pe_date
          LEFT OUTER JOIN `hca-hin-prod-cur-pub.edw_pub_views.fact_facility` AS ff_0 ON nbt.coid = ff_0.coid
          LEFT OUTER JOIN `hca-hin-prod-cur-psg.edwps_efr_views.comast_nbt_supplement` AS comast ON nbt.coid = comast.coid
        WHERE nbt.pe_date = (
          SELECT
              max(pe_date)
            FROM
              `hca-hin-prod-cur-psg.edwps_efr_views.gl_coid_department_provider` --  prior month
        )
         AND relt.coid_dept_relt_id IN(
          
          2,3,5,17,8,20,24,25,26,27
        )
         AND (upper(rtrim(nbt.provider_status_ind)) = 'A'
         OR upper(rtrim(nbt.provider_status_ind)) = 'T'
         AND nbt.status_change_date >= nbt.pe_date)
         AND cast(nbt.budget_cc_ind as INT64) = 0
         AND cast(nbt.is_approved_ind as INT64) = 1
         AND pro.original_contract_start_date <= nbt.pe_date
         AND upper(rtrim(ff_0.lob_code)) = 'HBS'
         AND upper(rtrim(comast.service_line_code)) = 'HOSP'
        QUALIFY row_number() OVER (PARTITION BY pro.npi,comast.hospital_coid ORDER BY relt.coid_dept_relt_cat_id, relt.coid_dept_relt_desc, nbt.provider_status_ind, nbt.provider_relationship_name, pro.original_contract_start_date) = 1
    ) AS valesco ON cast(fpd.attending_md_npi as string) = valesco.npi
 --   and fpd.coid = valesco.hospital_coid

        WHERE fp.Casemix_Exempt_Indicator = 'N'
            and fp.Patient_Type_Code_Pos1 = 'I'
              AND fpd.Discharge_Date <= last_day(date_add(current_date(), interval -1 MONTH))
  AND fpd.Final_Bill_Date <= current_date()
        GROUP BY 1, 2, 3, 4, 5,6,8, 9--, 10, 11, 12, 13, 14, 15, 16, 17, 18,19,20,21,22
    ) AS p
    LEFT JOIN  `hca-hin-prod-cur-pub.edw_pub_views.lu_date` AS lu 
        ON p.discharge_date = lu.date_id
    LEFT JOIN `hca-hin-prod-cur-pub.edw_pub_views.fact_facility` AS ff 
        ON p.coid = ff.coid

----------------------------------Physician Satisfaction Overall Rating of Care---------------------------
      LEFT JOIN   (
       SELECT
          pex.qtr_id,
          pex.parent_coid AS coid,
          pex.physician_npi,
          pex.survey_category_code,
          bi.question_id,
          pex.question_short_name,
          sum(pex.score_numerator_num) AS score_num,
          sum(pex.total_response_count_num) AS score_den,
          ROUND(CAST(sum(pex.score_numerator_num) as NUMERIC) / CAST(sum(pex.total_response_count_num) as NUMERIC), 4, 'ROUND_HALF_EVEN') AS pat_sat_score
      FROM `hca-hin-prod-cur-clinical.edwci_aggpsat_views.pex_npi` AS pex
          LEFT JOIN (
            SELECT DISTINCT
                bi_psat_dept_level_smry.parent_coid,
                bi_psat_dept_level_smry.qtr_id,
                bi_psat_dept_level_smry.survey_category_code,
                bi_psat_dept_level_smry.survey_sub_category_text,
                bi_psat_dept_level_smry.question_id,
                bi_psat_dept_level_smry.question_short_name
              FROM    `hca-hin-prod-cur-clinical.edwci_aggpsat_views.bi_psat_dept_level_smry` AS bi_psat_dept_level_smry
          ) AS bi ON pex.parent_coid = bi.parent_coid
           AND pex.qtr_id = bi.qtr_id
           AND pex.question_short_name = bi.question_short_name
           AND pex.survey_category_code = bi.survey_category_code
           AND pex.survey_sub_category_text = bi.survey_sub_category_text
        WHERE upper(rtrim(pex.role_type_code)) = 'ATT'
         AND upper(rtrim(pex.survey_category_code)) = 'IN'
         and PEX.Question_Short_Name like ANY (
          'Overall rating of care',
          'Doctors informative re treatment',
          'Doctors% concern for comfort',
          'Doctors took time to listen',
          'Courtesy of doctors',
          'Doctors include you trtmt decision',
          'Doctors treat with courtesy/respect',
          'Doctors kept you informed',
          'Doctors% concern questions/worries',
          'Time doctors spent with you',
          'Doctors expl in way you understand',
          'Doctors listen carefully to you',
          'Rate hospital 0-10')
        GROUP BY 1, 2, 3, 4, 5, 6
       ) AS ps ON ps.physician_npi = p.attending_md_npi   
         AND ps.coid = p.coid
        AND ps.qtr_id = lu.qtr_id

Left join `hca-hin-prod-cur-psg.edwps_efr_views.comast_nbt_supplement` nbt
  on p.coid = nbt.hospital_coid
  and nbt.lob_code = 'HBS'
  and nbt.sub_service_line_code = 'Hosp'

    
  WHERE lu.pe_date BETWEEN '2025-01-31' AND last_day(date_add(current_date(), interval -1 MONTH))
  AND p.Valesco_Provider = 'Y'
 -- and p.coid = '30932'
  --and p.attending_md_npi = 1184201832

     -- and FF.Division_Name = 'NORTH TEXAS DIVISION'
     --   and p.Attending_MD_NPI =  1710112370
  GROUP BY 1, 2, 3, 4, 5, 6--, 7,8,9, 10,11,12, 13,14,15,16