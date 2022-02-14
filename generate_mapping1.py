#!/usr/bin/env python3
import os
import sys
import pickle
import cms

SUB_LOOKUP = {
'den': [
    ('buyin', ['ENTITLEMENT_BUY_IN_IND']),
    ('hmoind', ['HMO_INDICATOR']),
],
'inp': [
    ('dgns', ['ICD_DGNS_CD', 'CLM_POA_IND_SW']),
    ('dgns_e', ['ICD_DGNS_E_CD', 'CLM_E_POA_IND_SW']),
    ('prcdr', ['ICD_PRCDR_CD', 'PRCDR_DT']),
 ],
'out': [
    ('dgns', ['ICD_DGNS_CD']),
    ('dgns_e', ['ICD_DGNS_E_CD']),
    ('prcdr', ['ICD_PRCDR_CD', 'PRCDR_DT']),
],
'snf':[
    ('dgns', ['ICD_DGNS_CD']),
    ('dgns_e', ['ICD_DGNS_E_CD']),
    ('prcdr', ['ICD_PRCDR_CD', 'PRCDR_DT']),
],
'hosp':[
    ('dgns', ['ICD_DGNS_CD']),
    ('dgns_e', ['ICD_DGNS_E_CD']),
],
'hha': [
    ('dgns', ['ICD_DGNS_CD']),
    ('dgns_e', ['ICD_DGNS_E_CD']),
],
'car': [
    ('dgns', ['ICD_DGNS_CD']),
],
'dme': [
    ('dgns', ['ICD_DGNS_CD']),
]
}

# According to LDS_SAF_2008_Changes.PDF, these fields are totally NULL since 2008 in format H
OLD_TOTALLY_NULL = {
'inp': [
        "CLM_DGNS_E_CD",
        "CLM_EXCPTD_NEXCPTD_TRTMT_CD",
        "CLM_PPS_CPTL_DSCHRG_FRCTN_PCT",
        "CLM_PPS_CPTL_HSP_AMT",
        "CLM_TRANS_CD",
        "FI_RQST_CLM_CNCL_RSN_CD",
        "NCH_BLOOD_DDCTBL_PT_QTY",
        "NCH_BLOOD_NCOV_CHRG_AMT",
        "NCH_BLOOD_PT_NRPLC_QTY",
        "NCH_BLOOD_PT_RPLC_QTY",
        "NCH_BLOOD_TOT_CHRG_AMT",
        "NCH_PMT_EDIT_RIC_CD",
        "NCH_QLFY_STAY_THRU_DT",
        "REV_CNTR_DT",
        "REV_CNTR_HCPCS_INITL_MDFR_CD",
        "REV_CNTR_HCPCS_2ND_MDFR_CD",
        "REV_CNTR_HCPCS_3RD_MDFR_CD",
        "REV_CNTR_HCPCS_4TH_MDFR_CD",
        "REV_CNTR_HCPCS_5TH_MDFR_CD",
        "REV_CNTR_PMT_MTHD_IND_CD",
        "REV_CNTR_DSCNT_IND_CD",
        "REV_CNTR_PACKG_IND_CD",
        "REV_CNTR_PRICNG_IND_CD",
        "REV_CNTR_OTAF_1_IND_CD",
        "REV_CNTR_IDE_NDC_UPC_NUM",
        "REV_CNTR_BLOOD_DDCTBL_AMT",
        "REV_CNTR_CASH_DDCTBL_AMT",
        "REV_CNTR_WAGE_ADJSTD_COINS_AMT",
        "REV_CNTR_RDCD_COINS_AMT",
        "REV_CNTR_MSP1_PD_AMT",
        "REV_CNTR_MSP2_PD_AMT",
        "REV_CNTR_PRVDR_PMT_AMT",
        "REV_CNTR_BENE_PMT_AMT",
        "REV_CNTR_PTNT_RESP_PMT_AMT",
        "REV_CNTR_PMT_AMT",
 ],
'out': [
        "CLM_DGNS_E_CD",
        "CLM_EXCPTD_NEXCPTD_TRTMT_CD",
        "CLM_OP_BENE_INTRM_DDCTBL_AMT",
        "CLM_OP_ESRD_MTHD_REIMBRSMT_CD",
        "CLM_OP_RFRL_CD",
        "CLM_OP_SRVC_TYPE_CD",
        "CLM_OP_TRANS_TYPE_CD",
        "CLM_PPS_IND_CD",
        "CLM_TRANS_CD",
        "FI_RQST_CLM_CNCL_RSN_CD",
        "NCH_BLOOD_DDCTBL_PT_QTY",
        "NCH_BLOOD_PT_FRNSH_QTY",
        "NCH_BLOOD_PT_NRPLC_QTY",
        "NCH_BLOOD_PT_RPLC_QTY",
        "NCH_PMT_EDIT_RIC_CD",
        "REV_CNTR_HCPCS_3RD_MDFR_CD",
        "REV_CNTR_HCPCS_4TH_MDFR_CD",
        "REV_CNTR_HCPCS_5TH_MDFR_CD",
        "REV_CNTR_DDCTBL_COINSRNC_CD",
],
'snf':[
        "BENE_LRD_USE_CNT",
        "CLM_DGNS_E_CD",
        "CLM_DRG_OUTLIER_STAY_CD",
        "CLM_EXCPTD_NEXCPTD_TRTMT_CD",
        "CLM_PASS_THRU_PER_DIEM_AMT",
        "CLM_PPS_CPTL_DSCHRG_FRCTN_PCT",
        "CLM_PPS_CPTL_DRG_WT_NUM",
        "CLM_PPS_CPTL_HSP_AMT",
        "CLM_TOT_PPS_CPTL_AMT",
        "CLM_TRANS_CD",
        "FI_RQST_CLM_CNCL_RSN_CD",
        "NCH_BLOOD_DDCTBL_PT_QTY",
        "NCH_BLOOD_NCOV_CHRG_AMT",
        "NCH_BLOOD_PT_NRPLC_QTY",
        "NCH_BLOOD_PT_RPLC_QTY",
        "NCH_BLOOD_TOT_CHRG_AMT",
        "NCH_DRG_OUTLIER_APRV_PMT_AMT",
        "NCH_PMT_EDIT_RIC_CD",
        "NCH_PROFNL_CMPNT_CHRG_AMT",
        "REV_CNTR_DT",
        "REV_CNTR_APC_HIPPS_CD",
        "REV_CNTR_HCPCS_INITL_MDFR_CD",
        "REV_CNTR_HCPCS_2ND_MDFR_CD",
        "REV_CNTR_HCPCS_3RD_MDFR_CD",
        "REV_CNTR_HCPCS_4TH_MDFR_CD",
        "REV_CNTR_HCPCS_5TH_MDFR_CD",
        "REV_CNTR_PMT_MTHD_IND_CD",
        "REV_CNTR_DSCNT_IND_CD",
        "REV_CNTR_PACKG_IND_CD",
        "REV_CNTR_PRICNG_IND_CD",
        "REV_CNTR_OTAF_1_IND_CD",
        "REV_CNTR_IDE_NDC_UPC_NUM",
        "REV_CNTR_BLOOD_DDCTBL_AMT",
        "REV_CNTR_CASH_DDCTBL_AMT",
        "REV_CNTR_WAGE_ADJSTD_COINS_AMT",
        "REV_CNTR_RDCD_COINS_AMT",
        "REV_CNTR_MSP1_PD_AMT",
        "REV_CNTR_MSP2_PD_AMT",
        "REV_CNTR_PRVDR_PMT_AMT",
        "REV_CNTR_BENE_PMT_AMT",
        "REV_CNTR_PTNT_RESP_PMT_AMT",
        "REV_CNTR_PMT_AMT",
],
'hosp':[
        "CLM_DGNS_E_CD",
        "CLM_EXCPTD_NEXCPTD_TRTMT_CD",
        "CLM_MCO_PD_SW",
        "CLM_OPRTG_PHYSN_UPIN_NUM",
        "CLM_OTHR_PHYSN_UPIN_NUM",
        "CLM_PPS_IND_CD",
        "CLM_TRANS_CD",
        "FI_RQST_CLM_CNCL_RSN_CD",
        "NCH_PMT_EDIT_RIC_CD",
        "CLM_PRCDR_CD",
        "CLM_PRCDR_PRFRM_DT",
        "REV_CNTR_APC_HIPPS_CD",
        "REV_CNTR_HCPCS_3RD_MDFR_CD",
        "REV_CNTR_HCPCS_4TH_MDFR_CD",
        "REV_CNTR_HCPCS_5TH_MDFR_CD",
        "REV_CNTR_PMT_MTHD_IND_CD",
        "REV_CNTR_DSCNT_IND_CD",
        "REV_CNTR_PACKG_IND_CD",
        "REV_CNTR_PRICNG_IND_CD",
        "REV_CNTR_OTAF_1_IND_CD",
        "REV_CNTR_IDE_NDC_UPC_NUM",
        "REV_CNTR_BLOOD_DDCTBL_AMT",
        "REV_CNTR_CASH_DDCTBL_AMT",
        "REV_CNTR_WAGE_ADJSTD_COINS_AMT",
        "REV_CNTR_RDCD_COINS_AMT",
        "REV_CNTR_MSP1_PD_AMT",
        "REV_CNTR_MSP2_PD_AMT",
        "REV_CNTR_PTNT_RESP_PMT_AMT",
],
'hha': [
        "CLM_DGNS_E_CD",
        "CLM_EXCPTD_NEXCPTD_TRTMT_CD",
        "CLM_MCO_PD_SW",
        "CLM_OPRTG_PHYSN_UPIN_NUM",
        "CLM_OTHR_PHYSN_UPIN_NUM",
        "CLM_TRANS_CD",
        "FI_RQST_CLM_CNCL_RSN_CD",
        "NCH_BENE_DSCHRG_DT",
        "NCH_PMT_EDIT_RIC_CD",
        "NCH_QLFY_STAY_THRU_DT",
        "REV_CNTR_HCPCS_3RD_MDFR_CD",
        "REV_CNTR_HCPCS_4TH_MDFR_CD",
        "REV_CNTR_HCPCS_5TH_MDFR_CD",
        "REV_CNTR_DSCNT_IND_CD",
        "REV_CNTR_PACKG_IND_CD",
        "REV_CNTR_PRICNG_IND_CD",
        "REV_CNTR_OTAF_1_IND_CD",
        "REV_CNTR_IDE_NDC_UPC_NUM",
        "REV_CNTR_BLOOD_DDCTBL_AMT",
        "REV_CNTR_CASH_DDCTBL_AMT",
        "REV_CNTR_WAGE_ADJSTD_COINS_AMT",
        "REV_CNTR_RDCD_COINS_AMT",
        "REV_CNTR_MSP1_PD_AMT",
        "REV_CNTR_MSP2_PD_AMT",
        "REV_CNTR_PRVDR_PMT_AMT",
        "REV_CNTR_BENE_PMT_AMT",
        "REV_CNTR_PTNT_RESP_PMT_AMT",
],
'car': [
        "CPO_PRVDR_NUM",
        "CARR_CLM_BENE_PD_AMT",
        "CLM_BLOOD_DDCTBL_PT_QTY",
        "CLM_BLOOD_PT_FRNSH_QTY",
        "CLM_EXCPTD_NEXCPTD_TRTMT_CD",
        "LINE_IDE_NUM",
        "LINE_NATL_DRUG_CD",
        "CARR_LINE_PSYCH_OT_PT_LMT_AMT",
        "LINE_INTRST_AMT",
        "LINE_PRMRY_PYR_ALOW_CHRG_AMT",
        "LINE_10PCT_PNLTY_RDCTN_AMT",
        "CARR_LINE_BLOOD_DDCTBL_QTY",
        "CARR_LINE_CLNCL_LAB_NUM",
        "CARR_LINE_CLNCL_LAB_CHRG_AMT",
        "LINE_PMT_IND_CD",
        "CARR_LINE_CLIA_ALERT_IND_CD",
        "LINE_DME_PURC_PRICE_AMT",
],
'dme': [
        "CARR_CLM_BENE_PD_AMT",
        "CLM_EXCPTD_NEXCPTD_TRTMT_CD",
        "LINE_IDE_NUM",
        "LINE_INTRST_AMT",
        "LINE_10PCT_PNLTY_RDCTN_AMT",
        "LINE_PMT_IND_CD",
        "DMERC_LINE_SCRN_SUSPNSN_IND_CD",
        "DMERC_LINE_SCRN_RSLT_IND_CD",
        "DMERC_LINE_WVR_PRVDR_LBLTY_SW",
        "DMERC_LINE_DCSN_IND_SW",
]
}

MAPPING = [('ICD_DGNS_CD', 'CLM_DGNS_CD'),
           ('GNDR_CD', 'BENE_SEX_IDENT_CD'), # 需要确认新格式男1女2
           ('DOB_DT', 'BENE_BIRTH_DT'),      # 需要确认格式
           ('BENE_CNTY_CD', 'BENE_RSDNC_SSA_STD_CNTY_CD'),
           ('BENE_STATE_CD', 'BENE_RSDNC_SSA_STD_STATE_CD'),
           ('PRNCPAL_DGNS_CD', 'CLM_PRNCPAL_DGNS_CD'),
           ('ICD_DGNS_E_CD', 'CLM_DGNS_CD'), # ICD-E code is different from ICD
           ('ICD_PRCDR_CD', 'CLM_PRCDR_CD'), # 需要验证
           ('PRCDR_DT', 'CLM_PRCDR_PRFRM_DT'),
           ('CLM_POA_IND_SW', ['CLM_POA_IND_SW', 'CLM_DGNS_CD']), # 需要验证 Claim Diagnosis Code II Diagnosis Present on Admission Indicator Code
           ('CLM_E_POA_IND_SW', 'CLM_DGNS_CD'), # 需要验证 Claim Diagnosis Code II Diagnosis Present on Admission Indicator Code
           ('NCH_NEAR_LINE_REC_IDENT_CD', 'NCH_NEAR_LINE_RIC_CD'),
           ('NCH_CLM_TYPE_CD', ['UTLHOSPI_NCH_CLM_TYPE_CD', 'UTLOUTPI_NCH_CLM_TYPE_CD', 'UTLIPSNI_NCH_CLM_TYPE_CD', 'UTLHHAI_NCH_CLM_TYPE_CD', 'UTLDMERI_NCH_CLM_TYPE_CD', 'UTLCARRI_NCH_CLM_TYPE_CD']), # TODO
           ('AT_PHYSN_UPIN', 'CLM_ATNDG_PHYSN_UPIN_NUM'),
           ('OP_PHYSN_UPIN', 'CLM_OPRTG_PHYSN_UPIN_NUM'),
           ('OT_PHYSN_UPIN', 'CLM_OTHR_PHYSN_UPIN_NUM'),
           ('AT_PHYSN_NPI', 'CLM_ATNDG_PHYSN_NPI_NUM'),
           ('OP_PHYSN_NPI', 'CLM_OPRTG_PHYSN_NPI_NUM'),
           ('OT_PHYSN_NPI', 'CLM_OTHR_PHYSN_NPI_NUM'),
           ('CLAIM_QUERY_CODE', 'CLM_QUERY_CD'),
           ('CLM_MDCR_NON_PMT_RSN_CD', 'CLM_MDCR_NPMT_RSN_CD'),
           ('PRVDR_STATE_CD', ['NCH_PRVDR_STATE_CD', 'LINE_NCH_PRVDR_STATE_CD', 'DMERC_LINE_PRVDR_STATE_CD']),
           ('NCH_BLOOD_PNTS_FRNSHD_QTY', ['CLM_BLOOD_PT_FRNSH_QTY','NCH_BLOOD_PT_FRNSH_QTY']),
           #('CLM_IP_ADMSN_TYPE_CD', 'CLM_SRC_IP_ADMSN_CD'),
           ('NCH_PTNT_STATUS_IND_CD', 'NCH_PTNT_STUS_IND_CD'),
           ('CLM_HOSPC_START_DT_ID', 'CLM_HOSPC_STRT_DT'),
           ('NCH_BENE_BLOOD_DDCTBL_LBLTY_AM', 'NCH_BENE_BLOOD_DDCTBL_AMT'),
           ('RFR_PHYSN_NPI', ['CARR_CLM_RFRG_NPI_NUM', 'RFR_PHYSN_NPI', 'DMERC_CLM_ORDRG_PHYSN_NPI_NUM']),
           ('RFR_PHYSN_UPIN', ['CARR_CLM_RFRG_UPIN_NUM', 'RFR_PHYSN_UPIN', 'DMERC_CLM_ORDRG_PHYSN_UPIN_NUM']),

           ('ADMTG_DGNS_CD', 'CLM_ADMTG_DGNS_CD'),
           ('CARR_CLM_RFRNG_PIN_NUM', 'CARR_CLM_RFRG_PIN_NUM'),
           ('NCH_CARR_CLM_SBMTD_CHRG_AMT', 'NCH_CARR_SBMT_CHRG_AMT'),
           ('NCH_CARR_CLM_ALOWD_AMT', 'NCH_CARR_ALOW_CHRG_AMT'),
           ('CARR_CLM_CASH_DDCTBL_APLD_AMT', 'CARR_CLM_CASH_DDCTBL_APPLY_AMT'),
           ('NCH_BENE_PTA_COINSRNC_LBLTY_AM', 'NCH_BENE_PTA_COINSRNC_AMT'),
           ('BENE_TOT_COINSRNC_DAYS_CNT', 'BENE_TOT_COINSRNC_DAY_CNT'),
           ('BENE_LRD_USED_CNT', 'BENE_LRD_USE_CNT'),
           ('CLM_NON_UTLZTN_DAYS_CNT', 'CLM_NUTLZTN_DAY_CNT'),
           ('NCH_DRG_OUTLIER_APRVD_PMT_AMT', 'NCH_DRG_OUTLIER_APRV_PMT_AMT'),
           ('NCH_QLFYD_STAY_THRU_DT', 'NCH_QLFY_STAY_THRU_DT'),
           ('REV_CNTR', 'REV_CNTR_CD'),                        # 以下都是lines
           ('HCPCS_CD', ['LINE_HCPCS_CD', 'REV_CNTR_HCPCS_CD']),
           ('REV_CNTR_NCVRD_CHRG_AMT', 'REV_CNTR_NCVR_CHRG_AMT'),
           ('REV_CNTR_1ST_MSP_PD_AMT', 'REV_CNTR_MSP1_PD_AMT'),
           ('REV_CNTR_2ND_MSP_PD_AMT', 'REV_CNTR_MSP2_PD_AMT'),
           ('HCPCS_1ST_MDFR_CD', ['REV_CNTR_HCPCS_INITL_MDFR_CD', 'LINE_HCPCS_INITL_MDFR_CD']),
           ('HCPCS_2ND_MDFR_CD', ['REV_CNTR_HCPCS_2ND_MDFR_CD', 'LINE_HCPCS_2ND_MDFR_CD']),
           ('REV_CNTR_PMT_AMT_AMT', 'REV_CNTR_PMT_AMT'),
           ('REV_CNTR_STUS_IND_CD', 'NCH_PTNT_STUS_IND_CD'),    # ???
           ('REV_CNTR_OTAF_PMT_CD', 'REV_CNTR_OTAF_1_IND_CD'),
           ('REV_CNTR_COINSRNC_WGE_ADJSTD_C', 'REV_CNTR_WAGE_ADJSTD_COINS_AMT'),
           ('REV_CNTR_RDCD_COINSRNC_AMT', 'REV_CNTR_RDCD_COINS_AMT'),
           ('REV_CNTR_PTNT_RSPNSBLTY_PMT', 'REV_CNTR_PTNT_RESP_PMT_AMT'),
           ('PRVDR_SPCLTY', 'LINE_HCFA_PRVDR_SPCLTY_CD'),
           ('LINE_ICD_DGNS_CD', 'LINE_DGNS_CD'),
           ('BETOS_CD', 'LINE_NCH_BETOS_CD'),
           ('LINE_SBMTD_CHRG_AMT', 'LINE_SBMT_CHRG_AMT'),
           ('CARR_PRFRNG_PIN_NUM', 'CARR_LINE_PRFRMG_PIN_NUM'),
           ('PRF_PHYSN_UPIN', 'CARR_LINE_PRFRMG_UPIN_NUM'),     # CHECK
           ('PRF_PHYSN_NPI', 'CARR_LINE_PRFRMG_NPI_NUM'),       # CHECK
           ('ORG_NPI_NUM', ['ORG_NPI_NUM', 'CARR_LINE_PRFRMG_GRP_NPI_NUM']),     # CHECK
           ('PRTCPTNG_IND_CD', 'LINE_PRVDR_PRTCPTG_IND_CD'),
           ('CARR_LINE_RDCD_PMT_PHYS_ASTN_C', 'CARR_LINE_RDCD_PHYSN_ASTNT_CD'),
           ('LINE_CMS_TYPE_SRVC_CD', 'LINE_HCFA_TYPE_SRVC_CD'),
           ('LINE_PLACE_OF_SRVC_CD', 'LINE_PLC_SRVC_CD'),
           ('LINE_BENE_PTB_DDCTBL_AMT', ['LINE_BENE_PTB_DDCTBL_AMT', 'LINE_BENE_PB_DDCTBL_AMT']),
           ('LINE_ALOWD_CHRG_AMT', 'LINE_ALOW_CHRG_AMT'),
           ('LINE_SERVICE_DEDUCTIBLE', 'LINE_SRVC_DDCTBL_IND_SW'),
           ('CARR_LINE_MTUS_CD', 'CARR_LINE_MTUS_IND_CD'),
           ('LINE_PRMRY_ALOWD_CHRG_AMT', 'LINE_PRMRY_PYR_ALOW_CHRG_AMT'),
           ('LINE_DME_PRCHS_PRICE_AMT', 'LINE_DME_PURC_PRICE_AMT'),
           ('PRVDR_NPI', 'DMERC_LINE_SUPLR_NPI_NUM'),
           ('DMERC_LINE_MTUS_CD', 'DMERC_LINE_MTUS_IND_CD'),
           ('LINE_NDC_CD', 'LINE_NATL_DRUG_CD'),
           ('REV_CNTR_DDCTBL_COINSRNC_CD', 'REV_CNTR_DDCTBL_COINSRNC_CD'),
           ('PRVDR_NUM', ['PRVDR_NUM', 'DMERC_LINE_SUPLR_PRVDR_NUM']),
          ]
MAPPING = dict(MAPPING)

IGNORE_OLD = ['DMERC_LINE_HCPCS_3RD_MDFR_CD',
              'DMERC_LINE_HCPCS_4TH_MDFR_CD',
              'NCH_NEAR_LINE_REC_VRSN_CD',
              'CARR_CLM_DGNS_CD_CNT',
              'DMERC_CLM_DGNS_CD_CNT',
              'HHA_CLM_DGNS_CD_CNT',
              'IP_CLM_DGNS_CD_CNT',
              'CARR_CLM_LINE_CNT',
              'DMERC_CLM_LINE_CNT',
              'CLM_TOT_SGMT_NUM',
              'CLM_TOT_SGMT_CNT',
              'CLM_SGMT_NUM',
              'OP_CLM_DGNS_CD_CNT',
              'OP_CLM_PRCDR_CD_CNT',
              'OP_CLM_RLT_COND_CD_CNT',
              'OP_CLM_RLT_OCRNC_CD_CNT',
              'OP_CLM_VAL_CD_CNT',
              'OP_REV_CNTR_CD_I_CNT',
              'IP_CLM_DGNS_CD_CNT',
              'IP_CLM_PRCDR_CD_CNT',
              'IP_CLM_RLT_COND_CD_CNT',
              'IP_CLM_RLT_OCRNC_CD_CNT',
              'IP_CLM_VAL_CD_CNT',
              'IP_REV_CNTR_CD_I_CNT',
              'HHA_CLM_DGNS_CD_CNT',
              'HHA_CLM_PRCDR_CD_CNT',
              'HHA_CLM_RLT_COND_CD_CNT',
              'HHA_CLM_RLT_OCRNC_CD_CNT',
              'HHA_CLM_VAL_CD_CNT',
              'HHA_REV_CNTR_CD_I_CNT',
              'HOSPC_CLM_DGNS_CD_CNT',
              'HOSPC_CLM_DGNS_CD_I_CNT',
              'HOSPC_CLM_PRCDR_CD_CNT',
              'HOSPC_CLM_RLT_COND_CD_CNT',
              'HOSPC_CLM_RLT_OCRNC_CD_CNT',
              'HOSPC_CLM_VAL_CD_CNT',
              'HOSPC_REV_CNTR_CD_I_CNT',
        ]
IGNORE_NEW = [
              'CARR_LINE_PRVDR_TYPE_CD',
              'FST_DGNS_E_CD',          # First Claim Diagnosis E Code
              'RSN_VISIT_CD',           # Reason for Visit Diagnosis Code 
              #'CLM_ADMSN_DT',           # Claim HHA Care Start Date
                                        # 或许可以根据以下两个倒推
                                        #   NCH_QLFY_STAY_THRU_DT
                                        #   NCH_BENE_DSCHRG_DT
              'CLM_DISP_CD',            # Claim Disposition Code 没找到
              'CARR_CLM_HCPCS_YR_CD',   # Carrier Claim HCPCS Year Code 没找到CAR
              'NCH_IP_NCVRD_CHRG_AMT',  # INP老的没有
              'NCH_VRFD_NCVRD_STAY_FROM_DT', # INP
              'NCH_VRFD_NCVRD_STAY_THRU_DT', # INP
              'NCH_BENE_MDCR_BNFTS_EXHTD_DT_I', #INP
              'RLT_VAL_CD_SEQ',         # snf_instval sequence
              'RLT_COND_CD_SEQ',        # snf_instcond
              'RLT_OCRNC_CD_SEQ',       # snf_instoccr
              'LINE_NUM',
              'CLM_LINE_NUM',
              'REV_CNTR_STUS_IND_CD',   # ?????
              'LINE_ICD_DGNS_VRSN_CD',
              'HCPCS_3RD_MDFR_CD',      # ????
              'HCPCS_4TH_MDFR_CD',
              'LINE_HCT_HGB_RSLT_NUM', 
              'LINE_HCT_HGB_TYPE_CD',
              'CARR_LINE_CLIA_LAB_NUM',     # MISS
              'CARR_LINE_ANSTHSA_UNIT_CNT', # MISS
              # 以下为显然可以直接扔掉的 
              'ICD_DGNS_VRSN_CD',
              'ICD_DGNS_E_VRSN_CD',
              'ICD_PRCDR_VRSN_CD',
              'ADMTG_DGNS_VRSN_CD',
              'FST_DGNS_E_VRSN_CD',
              'PRNCPAL_DGNS_VRSN_CD',
              'RSN_VISIT_VRSN_CD',
              ]

IGNORE_NEW_SUB = [  # 这些在claimsj里都有，不需要重复
            'DESY_SORT_KEY',
            'CLAIM_NO',
            'CLM_THRU_DT',
            'NCH_CLM_TYPE_CD'
        ]

ALLOW_NO_OLD = [
            'ICD_DGNS_E_CD',
            'CLM_E_POA_IND_SW'
        ]

def split_column_stem_suffix (orig_fname):
    # 如果没有suffix，返回suffix=0
    fname = orig_fname
    while fname[-1].isnumeric():
        fname = fname[:-1]
    suffix = orig_fname[len(fname):]
    if len(suffix) == 0:
        return fname, 0
    return fname, int(suffix)


# 确认2008-2010年的格式是一样的
def compare_format (f1, f2):
    assert len(f1) == len(f2)
    for a, b in zip(f1, f2):
        assert a.no == b.no
        assert a.long_name == b.long_name
        assert a.short_name == b.short_name
    pass

# 分析DEN格式
def analyze_denom ():
    fs1 = None
    for path in cms.deep_glob('den_saf_lds_5_*.fts'):
        fs = cms.load_columns(path)
        if fs1 is None:
            fs1 = fs
            continue
        compare_format(fs1, fs)
        pass
    fspec = []
    SUB = SUB_LOOKUP['den']
    subfs = [[] for _ in SUB]
    for f in fs1:
        stem, suffix = split_column_stem_suffix(f.long_name)
        used = False
        for (_, sub_col), subf in zip(SUB, subfs):
            if stem in sub_col:
                if suffix == 1:
                    subf.append((stem, f))
                used = True
        if not used:
            fspec.append([f.short_name, None, [(f.long_name, None)], f.line])
            pass
        pass
    for (name, _), subf in zip(SUB, subfs):
        fn = [f for f, _ in subf]
        newf = [f for f, _ in subf]
        lines = [f.line for _, f in subf]
        fspec.append([name, fn, [[None, newf]], lines])
        pass
    return ['den', ['den'], fspec]

def match_prefix (text, prefix):
    return text[:len(prefix)] == prefix

def analyze_claim_type (ctype):

    fs1 = None
    for path in cms.deep_glob('%s_clm_saf_lds_5_20*.fts' % ctype):
        fs = cms.load_columns(path)
        if fs1 is None:
            fs1 = fs
            continue
        compare_format(fs1, fs)
        pass
    print('%s 2008-2010: %d fields' % (ctype, len(fs1)))

    lookup = {} # 所有2008年的字段

    # 所有的字段都只按STEM处理
    # 类似ICD_PRCDR_CD2或者之后的，都进入{new,old}_sub_columns, 不予处理

    new_sub_columns = []
    old_sub_columns = []
    matchings = []
    fields_spec = []
    ignore_cnt = 0
    new_cnt = 0

    for f in fs1:
        old_stem, old_suffix = split_column_stem_suffix(f.long_name)
        if old_suffix > 1:
            old_sub_columns.append(f)
            continue
        lookup[old_stem] = f
        pass


    claim_paths = cms.deep_glob('%s_claims[jk]_lds_5_2011.fts' % ctype) + cms.deep_glob('%s_[^c]*_lds_5_2011.fts' % ctype)
    # 先看claimsj (is_master)

    for claim_path in claim_paths:
        print('\t',claim_path)
        fs = cms.load_columns(claim_path)
        table_name = claim_path.split('_')[1]
        is_master = (table_name == 'claimsj') or (table_name == 'claimsk')
        # 'claimsj'是主表, 别的是附表


        field_mapping = []

        for f in fs:
            new_short, _ = split_column_stem_suffix(f.short_name)
            if new_short in ['MDFR_CD', 'REV_MSP']:
                new_short = f.short_name
            #print('xxxx', new_short, f.long_name)
            new_stem, new_suffix = split_column_stem_suffix(f.long_name)
            if new_suffix > 1:
                new_sub_columns.append(f)
                # 不处理包含多列的字段, DGNS_CD2, ...
                continue

            if new_stem in IGNORE_NEW:
                ignore_cnt += 1
                continue

            trials = [new_stem]
            # 可以尝试的老名字
            old = MAPPING.get(new_stem, None)
            if type(old) is str:
                trials = [old]
            elif type(old) is list:
                trials = old

            found = False
            for old_stem in trials:
                if old_stem in lookup:
                    matchings.append([f, lookup[old_stem]])
                    field_mapping.append([new_short, new_stem, old_stem, new_suffix, f.line])
                    del lookup[old_stem]
                    found = True
                    break
                pass

            if (not is_master) and (new_stem in IGNORE_NEW_SUB):
                found = True
                pass

            if new_stem in ALLOW_NO_OLD:
                field_mapping.append([new_short, new_stem, None, new_suffix, f.line])
                found = True

            if not found:
                new_cnt += 1
                print('missing_new', f.long_name)
                pass
            pass

        if is_master:
            SUB = SUB_LOOKUP[ctype]     # 需要分离指定的子表
            sub_mappings = [[] for _ in SUB]
            for new_short, new, old, new_suffix, line in field_mapping:
                used = False
                for (_, cols), sub_mapping in zip(SUB, sub_mappings):
                    if new in cols:
                        used = True
                        assert new_suffix > 0
                        sub_mapping.append((new_short, new, old, line))
                        break
                if not used:
                    assert new_suffix == 0
                    assert not old is None, (new, old)
                    fields_spec.append([new_short, None, [[new, None], [new, None], [old, None]], line])
                pass
            for (name, cols), sub_mapping in zip(SUB, sub_mappings):
                assert len(cols) == len(sub_mapping), (ctype, name, cols, sub_mapping)
                fn = [a for a, _, _, _ in sub_mapping]
                newf = [a for _, a, _, _ in sub_mapping]
                oldf = [a for _, _, a, _ in sub_mapping]
                lines = [a for _, _, _, a in sub_mapping]
                # oldf 要么都是None，要么都不是
                if None in oldf:
                    for none in oldf:
                        assert none is None
                    fields_spec.append([name, fn, [[None, newf], [None, newf], [None, None]], lines])
                else:
                    fields_spec.append([name, fn, [[None, newf], [None, newf], [None, oldf]], lines])
        else:   # 是附表
            fn = [a for a, _, _, _, _ in field_mapping]
            newf = [a for _, a, _, _, _ in field_mapping]
            oldf = [b for _, _, b, _, _ in field_mapping]
            lines = [b for _, _, _, _, b in field_mapping]

            fields_spec.append([table_name, fn, [['%s_%s' % (ctype, table_name), newf], ['%s_%s' % (ctype, table_name), newf], [None, oldf]], lines])
        pass

    old_cnt = 0
    for k, _ in lookup.items():
        if not k in IGNORE_OLD + OLD_TOTALLY_NULL[ctype]:
            old_cnt += 1
            print('missing_old', k)

    return [ctype, ['%s_claimsk' % ctype, '%s_claimsj' % ctype, '%s' % ctype], fields_spec], new_sub_columns, old_sub_columns, matchings


def pad (xx):
    l = 24 - len(xx)
    if l > 0:
        return xx + ' ' * l
    return xx

specs = [analyze_denom()]
with open('mapping_supp.txt', 'w') as f, \
     open('mapping_sub.txt', 'w') as fsub: 
    for ct in cms.CLAIM_TYPES:
        f.write('%s\n' % ct)
        fsub.write('%s\n' % ct)
        spec, new_sub_columns, old_sub_columns, matchings = analyze_claim_type(ct)
        fsub.write('new_sub_columns:\n')
        for x in new_sub_columns:
            fsub.write('    %s\n' % x.line)
        fsub.write('old_sub_columns:\n')
        for x in old_sub_columns:
            fsub.write('    %s\n' % x.line)
        for n, o in matchings:
            if n.long_name != o.long_name:
                f.write('    %s\n' % n.line)
                f.write('    %s\n' % o.line)
                f.write('----\n')
                pass
            pass
        pass
        specs.append(spec)
        pass

#assert not os.path.exists(meta_name), "cms.meta exists"
with open('mapping.pkl', 'wb') as f:
    pickle.dump(specs, f)
    pass

# generate report
with open('mapping.txt', 'w') as f:
    for spec in specs:
        ctype, table_spec, fields = spec
        f.write('%s        %s\n' % (ctype, table_spec))
        for field in fields:
            fname, sub, versions, _ = field
            if sub is None:
                f.write('    %s    %s\n' % (pad(fname), [x for x, _ in versions]))
            else:
                f.write('    %s    %s\n' % (pad(fname), [v[0] for v in versions]))
                for i, sname in enumerate(sub):
                    f.write('        %s    %s\n' % (pad(sname), [None if v[1] is None else v[1][i] for v in versions]))
                    pass
        f.write('\n')
pass


