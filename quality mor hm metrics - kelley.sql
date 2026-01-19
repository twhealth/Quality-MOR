----Updated August 2025 by Kelley Kemp. Do not modify logic without confirming and reviewing with Kelley----

  SELECT
    ff.Group_Name,
    ff.Division_Name,
    ff.Market_Name,
    ff.COID_Name,
    p.COID,
    CASE WHEN f.Same_Store_Type_Code = 'CSS' THEN 'Same Store' ELSE 'Not Same Store' END AS Same_Store_Status,
    p.Attending_MD_NPI,
    p.Attending_MD_Name,
    p.Valesco_Provider,
    p.Hospitalist_Flag,
    p.PE_Date,
    p.Year_id,
--    disease_group_code,
  --  disease_group_desc,
 --   clinical_domain_code,
    pat_sat_score,
    count(distinct p.patient_dw_id) AS case_count,
    sum(p.encounter_los_amt) AS total_los,
    sum(CAST(p.encounter_los_amt_home as INT64)) AS total_los_home,
    sum(p.drg_geometric_los_amt) AS total_geometric_los,
    sum(CAST(p.geometric_los_amt_home as INT64)) AS total_geometric_los_home,
    sum(p.casemix_index) AS total_casemix_index,
    SUM(p.Comp_Outcome) AS complication_count,
    sum(p.Comp_Probability) AS complication_prob,
    sum(p.Mort_Outcome) AS mortality_count,
    sum(p.Mort_Probability) AS mortality_prob,
    sum(p.readmission_ind) AS readmission_count
  FROM
    -- --------------------------Patient Level Detail Summary-------------------------------------
    (
 SELECT
          cast(fpd.Patient_DW_ID as string) as Patient_DW_ID,
          fpd.pat_acct_num,
          fpd.coid,
          fpd.attending_md_npi,
          fpd.attending_md_name,
          max(CASE WHEN valesco.npi IS NOT NULL THEN 'Y' ELSE 'N'
            END) AS Valesco_Provider,
          max(hospitalist.hospitalist_flag) AS Hospitalist_Flag,
          lu.PE_Date,
    lu.Year_id,
          fpd.admission_date,
          fpd.discharge_date,
          fpd.Final_Bill_Date,
          fpd.patient_age_amt,
          fpd.discharge_status_code,
          comp.disease_group_code,
          ref_dg.disease_group_desc,
          ref_dg.clinical_domain_code,
          ref_cd.clinical_domain_desc,
          fpd.encounter_los_amt,
          CASE WHEN (fpd.discharge_status_code) = 1 THEN fpd.encounter_los_amt
            ELSE NULL
          END AS encounter_los_amt_home,
          fpd.drg_geometric_los_amt,
          CASE WHEN (fpd.discharge_status_code) = 1 THEN fpd.drg_geometric_los_amt
            ELSE NULL
          END AS geometric_los_amt_home,
          fpd.drg_payment_weight_amt AS casemix_index,
          comp.Comp_Outcome,
          comp.Comp_Probability,
          mort.Mort_Outcome,
          mort.Mort_Probability,
          CASE WHEN upper(rtrim(fpd.readmission_30_day_ind)) = 'Y' THEN 1
            ELSE 0 END AS readmission_ind,
            CAST(avg(ps.pat_sat_score) as FLOAT64) AS pat_sat_score
        FROM
 `hca-hin-prod-cur-clinical.edwcdm_pc_views.fact_ce_patient_detail_crnt`  AS fpd
        LEFT JOIN `hca-hin-prod-cur-ops.edwpf_views.fact_patient` fp
              ON FP.PATIENT_DW_ID = FPD.PATIENT_DW_ID
        LEFT JOIN  `hca-hin-prod-cur-pub.edw_pub_views.lu_date` AS lu 
             ON fpd.discharge_date = lu.date_id

  ---------------------------- Complications----------------------------------------------------------
        LEFT JOIN (
               SELECT
                patient_dw_id,
                disease_group_code,
               SAFE_CAST(observed_value AS NUMERIC) as Comp_Outcome,
               expected_value as Comp_Probability
              from `hca-hin-prod-cur-ops.edwpf_views.csa_outcome_riskscore`
             where outcome_text = 'COMPLICATION'
            qualify (row_number() over (partition by Patient_DW_ID order by model_year desc)=1)
                ) comp
                on fp.Patient_DW_ID=comp.Patient_DW_ID  
 -------------------------- Mortalities-------------------------------------------------------------
        LEFT JOIN (
                SELECT
                 patient_dw_id,
                 SAFE_CAST(observed_value AS NUMERIC)as Mort_Outcome,
                 expected_value as Mort_Probability
               from `hca-hin-prod-cur-ops.edwpf_views.csa_outcome_riskscore`
              where outcome_text = 'MORTALITY'
               qualify (row_number() over (partition by Patient_DW_ID order by model_year desc)=1)
                  ) mort
                on fp.Patient_DW_ID=mort.Patient_DW_ID         
            -- Disease Group Description

        LEFT JOIN `hca-hin-prod-cur-ops.edwpf_views.ref_disease_group` ref_dg
              on comp.disease_group_code = ref_dg.disease_group_code

          -- Clinical Domain Description

        LEFT JOIN `hca-hin-prod-cur-ops.edwpf_views.ref_csa_clinical_domain` ref_cd
              on ref_dg.clinical_domain_code = ref_cd.clinical_domain_code

      -- -------------------------------------------Valesco Provider Flag------------------------------------------------------
      LEFT JOIN 
      ( SELECT DISTINCT
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
    and fpd.coid = valesco.hospital_coid 

          LEFT OUTER JOIN 
    -- ---------------------------------------------Hospitalist Flag ----------------------------------------------------------------
    (
      SELECT
          prov.npi,
          prov.provider_name,
          CASE
            WHEN upper(rtrim(hbp.prov_category)) = 'HOSPITALIST' THEN 'Y'
            ELSE 'N'
          END AS hospitalist_flag
        FROM
          `hca-hin-prod-cur-psg.edwps_dss_views.provider_status_current` AS prov
          LEFT OUTER JOIN `hca-hin-prod-cur-psg.edwps_dss_views.hospitalist_system_location_hbp` AS hbp ON prov.npi = cast(hbp.npi as numeric)
    ) AS hospitalist ON fpd.attending_md_npi = hospitalist.npi    

    -- -------------------------Physician Satisfaction Overall Rating of Care---------------------------
       LEFT JOIN 
  (
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
         and PEX.Question_Short_Name LIKE ANY (
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
       ) AS ps ON ps.physician_npi = fpd.attending_md_npi   
         AND ps.coid = fpd.coid
        AND ps.qtr_id = lu.qtr_id  

  WHERE  lu.pe_date between'2024-01-01' AND last_day(date_add(current_date(), interval -1 MONTH))
 -- and fpd.coid = '26109'
 -- and fpd.attending_md_npi = 1710112370
 -- and fpd.patient_dw_id = 252049904301936047
        AND fp.Casemix_Exempt_Indicator = 'N'
        AND fp.Patient_Type_Code_Pos1 = 'I'
        AND fpd.Discharge_Date <= last_day(date_add(current_date(), interval -1 MONTH))
  --AND fpd.Final_Bill_Date <= current_date()
        GROUP BY 1, 2, 3, 4, 5,8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18,19,20,21,22,23,24,25,26,27,28) as p 
                LEFT JOIN `hca-hin-prod-cur-pub.edw_pub_views.fact_facility` AS ff 
                     ON p.coid = ff.coid
                LEFT JOIN hca-hin-prod-cur-pub.edw_pub_views.Facility  F
                     ON FF.Coid = F.Coid
  WHERE    p.Valesco_Provider = 'Y'
  and  p.Hospitalist_Flag = 'Y'
        and p.clinical_domain_code is not null
  GROUP BY 1, 2, 3, 4, 5, 6, 7,8,9, 10,11,12,13--,14,15 