# -*- coding: utf-8 -*-
"""
Created on Mon Feb  8 10:44:02 2021

@author: Kou Dena
"""

import pandas as pd
import datetime
from dateutil.relativedelta import relativedelta

############################################################################################################################################
# histEntityInfo = pd.read_csv('EntityInfoAll.csv')
# tdEntityInfo = self.EntityInfo # after self.ready = True, namely new mapping table from CRI has been loaded into database
# tdDate is as-of-date of PD with datetime format, e.g., on calendar date 20210111, tdDate = datetime.datetime(2021,1,10)

def compare_mapping_table(histEntityInfo, tdEntityInfo, tdDate):
    
    '''
    Parameters
    ----------
    histEntityInfo : DataFrame
        DESCRIPTION: read from EntityInfoAll.csv
        with columns ['CompanyCode','INDUSTRY','REGION','Database','CompanyName','REGION_Name','INDUSTRY_Name','UpdateDate','LastDate']
    tdEntityInfo : DataFrame
        DESCRIPTION: format is the same as histEntityInfo
         with columns ['CompanyCode','INDUSTRY','REGION','Database','CompanyName','REGION_Name','INDUSTRY_Name']
    tdDate : datetime
        DESCRIPTION: as-of-date of PD

    Returns
    -------
    histEntityInfo : DataFrame
        DESCRIPTION: updated one
        same with histEntityInfo
    '''  
    
    histEntityInfo['LastDate'] = pd.to_datetime(histEntityInfo['LastDate'])
    # if histEntityInfo['LastDate'].max()!=tdDate-datetime.timedelta(days=1):
    #     file = open('QA_Log_{}.txt'.format(tdDate.strftime('%Y%m%d')),'a')
    #     file.write('Historical entity information has suspicious LastDate, please have a check. \n')
    #     file.close()
    #     return
    
    histEntityInfo['UpdateDate'] = pd.to_datetime(histEntityInfo['UpdateDate'])
    histEntityInfo.sort_values(by=['CompanyCode','LastDate'],ascending=[True,True],inplace=True)
    ltHistEntityInfo = histEntityInfo.drop_duplicates(['CompanyCode'], keep='last') # last EntityInfo of each firm
    histEntityInfo.set_index(['CompanyCode','LastDate'],inplace=True)
    
    # historical EntityInfo except last EntityInfo
    histEntityInfo = histEntityInfo[~histEntityInfo.index.isin(ltHistEntityInfo.set_index(['CompanyCode','LastDate']).index)].reset_index() 
    
    tdEntityInfo['LastDate'] = tdDate
    tdEntityInfo['UpdateDate'] = tdDate
    ltHistEntityInfo = pd.concat([ltHistEntityInfo,tdEntityInfo])
    cols = list(ltHistEntityInfo.columns)
    cols.remove('LastDate')
    cols.remove('UpdateDate')
    ltHistEntityInfo.sort_values(by=['CompanyCode','LastDate'],ascending=[True,True],inplace=True)    
    
    # have EntityInfo yesterday and today, keep the first one if duplicate, keep both if not duplicate
    ltHistEntityInfo = ltHistEntityInfo.drop_duplicates(cols, keep='first').reset_index(drop=True)
    
    # have EntityInfo yesterday but missing today or have duplicated EntityInfo yesterday and today, keep yesterday's one and change LastDate to as-of-date of PD
    ltHistEntityInfo.loc[~ltHistEntityInfo.duplicated(subset='CompanyCode',keep=False),'LastDate'] = tdDate
    
    histEntityInfo = pd.concat([histEntityInfo,ltHistEntityInfo])
    histEntityInfo = histEntityInfo[cols+['UpdateDate','LastDate']].sort_values(by=['CompanyCode','LastDate'],ascending=[True,True])
    
    return histEntityInfo

# histEntityInfo.to_csv('EntityInfoAll.csv',index=False) # save the updated one for next day's QA
############################################################################################################################################


############################################################################################################################################
# Concatenate all riskFactor data sent by CRI
# tdRiskFactor = "D:/Budi/iRAP/Oct 2019 onwards/Product_Coordinator/QA_2021/RawDataFromCRI/Risk_Factor/"
# rfAll = pd.DataFrame([])
# for rfFile in os.listdir(tdRiskFactor):
#     rftemp = pd.read_csv(tdRiskFactor + rfFile)
#     rfAll = rfAll.append(rftemp,ignore_index=True)
# rfAll['DataDate'] = pd.to_datetime(rf['DataDate'])
    
# Concatenate all PD data calculated by iRAP
# tdPD = "D:/Budi/iRAP/Oct 2019 onwards/Product_Coordinator/QA_2021/PDdataFromCriAT/"
# pdAll = pd.DataFrame([])
# for region in ["IRAP_PUBLIC_AD","IRAP_PUBLIC_CN","IRAP_PUBLIC_EM","IRAP_PUBLIC_EU","IRAP_PUBLIC_IN","IRAP_PUBLIC_NA"]:
#     pdPath = tdPD +  str(region) + "/" + "PD_Individual/"
#     for pdFile in os.listdir(pdPath):
#         pdtemp = pd.read_csv(pdPath + pdFile)
#         pdAll = pdAll.append(pdtemp,ignore_index=True) 
# pdAll['DataDate'] = pd.to_datetime(pdAll['DataDate'])           
def get_entities_with_RF_no_PD(rfAll,pdAll,tdEntityInfo,tdDate):
    '''
    Parameters
    ----------
    rfAll : DataFrame
        DESCRIPTION: read from as-of-date risk factors data of all entities from CRI
        with columns ['CompanyID','DataDate','CalculationDate','Stock_Index_Return','Three_Month_Rate_After_Demean', 
                      'DTD_Level','DTD_Trend','Liquidity_Level_Nonfinancial','Liquidity_Trend_NonFinancial','NI_Over_TA_Level', 
                      'NI_Over_TA_Trend','Size_Level','Size_Trend','M_Over_B','SIGMA','Liquidity_Level_Financial', 
                      'Liquidity_Trend_Financial','DTD_Median_Fin','DTD_Median_Nfin','dummy_for_NM']  
    pdAll : DataFrame
        DESCRIPTION: as-of-date individual PD of all entities
        with columns ['CompanyCode','DataDate','Horizon','ForwardPoint','PD']
        
    tdEntityInfo : DataFrame
        DESCRIPTION: format is the same as histEntityInfo
         with columns ['CompanyCode','INDUSTRY','REGION','Database','CompanyName','REGION_Name','INDUSTRY_Name']

    Returns
    -------
    EtyWithRFnoPD : pd.DataFrame of entities with risk factors data but no PD
        COLUMNS:
                1. CompanyCode: IDBB of companies with suffix "IRAP_ENTITY_" (e.g. IRAP_ENTITY_12345)
                2. CntMissingRF: Number of missing risk factors (that cause the PD of the company to not be calculated)
    '''          
    
    # Extract CompanyID from pdAll and use it to filter out CompanyID in rfAll (no PD but have risk factors)   
    pdAll["CompanyID"] = pdAll["CompanyCode"].apply(lambda x: x[12:])
    pdAll["CompanyID"] = pd.to_numeric(pdAll["CompanyID"])
    pdAll['DataDate'] = pd.to_datetime(pdAll['DataDate'])
    pdAll = pdAll[(pdAll["Horizon"]=="12M")&(pdAll["ForwardPoint"]==0)]
    pdAll = pdAll[(~pdAll["PD"].isnull())&(pdAll["DataDate"]==tdDate)]
    # if pdAll.empty:
    #     file = open("QA_Log_{}.txt".format(tdDate.strftime("%Y%m%d")),"a")
    #     file.write("There is no as-of-date individual PD data. \n")
    #     file.close()
    
    CID_forPD = pdAll["CompanyID"].unique().tolist()
    
    rfAll['DataDate'] = pd.to_datetime(rfAll['DataDate'])
    rfAll = rfAll[rfAll["DataDate"]==tdDate]
    # if rfAll.empty:
    #     file = open("QA_Log_{}.txt".format(tdDate.strftime("%Y%m%d")),"a")
    #     file.write("There is no as-of-date individual risk factors data. \n")
    #     file.close()
        
    rfAll_noPD = rfAll.loc[~(rfAll["CompanyID"].isin(CID_forPD)),"CompanyID"].unique().tolist()
    
    # No change. Add one column in EtyWithRFnoPD (count number of missing risk factors)
    # Construct EtyWithRFnoPD that lists down all entities with RF but no PD.
    EtyWithRFnoPD = pd.DataFrame({"CompanyID":rfAll_noPD})
    EtyWithRFnoPD["CompanyCode"] = EtyWithRFnoPD["CompanyID"].apply(lambda x: "IRAP_ENTITY_"+str(x))
    
    RFlist = rfAll.columns.tolist()
    removeList = ["CompanyID","DataDate","CalculationDate"]
    RFlist = [x for x in RFlist if x not in removeList]
    
    rfAll_temp = rfAll.copy()
    rfAll_temp["CntMissingRF"] = rfAll_temp[RFlist].isnull().sum(axis=1)
    
    EtyWithRFnoPD = EtyWithRFnoPD.merge(rfAll_temp[["CompanyID","CntMissingRF"]],how="left",on="CompanyID")
    EtyWithRFnoPD = EtyWithRFnoPD.merge(tdEntityInfo[['CompanyCode','CompanyName','REGION_Name','INDUSTRY_Name']],how='left',on='CompanyCode')
    EtyWithRFnoPD = EtyWithRFnoPD[['CompanyCode','CompanyName','REGION_Name','INDUSTRY_Name','CntMissingRF']].reset_index(drop=True)
    EtyWithRFnoPD.sort_values(by='CntMissingRF',inplace=True)
    return EtyWithRFnoPD

# EtywithRfnoPD.to_csv（‘./Report/{}/EtyWithRFnoPD.csv’.format(tdDate.strftime('%Y%m%d')),index=False）
############################################################################################################################################

############################################################################################################################################
# activeDates = pd.read_csv("D:/Budi/iRAP/Oct 2019 onwards/Product_Coordinator/QA_2021/20210112_Dena/iRAP_Calculation/datesAll.csv")

# Concatenate all PD data calculated by iRAP
# pathTdyPD = "D:/Budi/iRAP/Oct 2019 onwards/Product_Coordinator/QA_2021/PDdataFromCriAT/"
# pdAll = pd.DataFrame([])
# for region in ["IRAP_PUBLIC_AD","IRAP_PUBLIC_CN","IRAP_PUBLIC_EM","IRAP_PUBLIC_EU","IRAP_PUBLIC_IN","IRAP_PUBLIC_NA"]:
#     pdPath = pathTdyPD +  str(region) + "/" + "PD_Individual/"
#     for pdFile in os.listdir(pdPath):
#         pdtemp = pd.read_csv(pdPath + pdFile)
#         pdAll = pdAll.append(pdtemp,ignore_index=True)    
# pdAll["DataDate"] = pd.to_datetime(pdAll["DataDate"])

# histEntityInfo = pd.read_csv("D:/Budi/iRAP/Oct 2019 onwards/Product_Coordinator/QA_2021/20210112_Dena/EntityInfoAll.csv")
# EtyWithRFnoPD = pd.read_csv("D:/Budi/iRAP/Oct 2019 onwards/Product_Coordinator/QA_2021/EtyWithRFnoPD_20210106.csv")

def compare_coverage_daily_PD_global(activeDates,pdAll,histEntityInfo,EtyWithRFnoPD,tdDate):
    
    '''
    Parameters
    ----------
    activeDates: pd.DataFrame
        DESCRIPTION: pd.DataFrame of entities with different date records with PD generated 
        COLUMNS: CompanyCode, DataDate
        EXAMPLE: datesAll.csv   
    pdAll : DataFrame
        DESCRIPTION: as-of-date individual PD of all entities
        with columns ['CompanyCode','DataDate','Horizon','ForwardPoint','PD']       
    histEntityInfo : pd.DataFrame
        DESCRIPTION: pd.DataFrame of historical mappingTable (to be used to compare with new entities)
        COLUMNS: CompanyCode, INDUSTRY (optional), REGION (optional), Database (optional), CompanyName (Optional), REGION_Name (Optional), INDUSTRY_Name (Optional), UpdateDate (Good to have), LastDate
        EXAMPLE: EntityInfoAll.csv
    EtyWithRFnoPD : pd.DataFrame
        DESCRIPTION: pd.DataFrame of entities with risk factors data but no PD (output from getEntitiesWithRFbutNoPD.py)
        COLUMNS: CompanyCode, CntMissingRF
        EXAMPLE: Output of getEntitiesWithRFbutNoPD
    
    tdDate : datetime
        DESCRIPTION: Specify today's date
        EXAMPLE: datetitme.datetime(2021,1,6)

    Returns
    -------
    MissingAndNew : pd.DataFrame of missing (have PD yesterday but not today) and new entities (have PD today but not yesterday) on tdDate
        COLUMNS:
                1. CompanyCode: IDBB of companies with suffix "IRAP_ENTITY_" (e.g. IRAP_ENTITY_12345)
                2. Flag: 1 (New entities and recorded in mappingTable [New Entities] or Missing due to incomplete RF [Missing Entities]), 
                    0 (New entities but not recorded in mappingTable [New Entities]  or Missing due to unknown reasons [Missing Entities])
                3. Status: "Missing" or "New" entities
    CmprActiveNum : pd.DataFrame of global all industries with yesterday and today's number of active entities (also includes absolute and percentage change)
        COLUMNS:
                1. REGION_Name: Global
                2. INDUSTRY_Name: All
                3. Yesterday: 1 day before tdDate (yesterday)
                4. YtdActiveNumber: number of active entities yesterday
                5. Today: tdDate (today)
                6. TdActiveNumber: number of active entitites today
                7. ChangeInNumber: Absolute change in number of active entities
                8. ChangeInPct: Percentage change in number of active entities
    '''          
     
    histEntityInfo["UpdateDate"] = pd.to_datetime(histEntityInfo["UpdateDate"])
    histEntityInfo["LastDate"] = pd.to_datetime(histEntityInfo["LastDate"])
    histEntityInfo = histEntityInfo[histEntityInfo["UpdateDate"]<=tdDate]
    activeDates["DataDate"] = pd.to_datetime(activeDates["DataDate"])
    
    pdAll["DataDate"] = pd.to_datetime(pdAll["DataDate"])
    pdAll = pdAll[(pdAll["Horizon"]=="12M")&(pdAll["ForwardPoint"]==0)]
    pdAll = pdAll[(~pdAll["PD"].isnull())&(pdAll["DataDate"]==tdDate)]
    # if pdAll.empty:
    #     file = open("QA_Log_{}.txt".format(tdDate.strftime("%Y%m%d")),"a")
    #     file.write("There is no as-of-date individual PD data. \n")
    #     file.close()
        
    # if histEntityInfo['LastDate'].max()!=tdDate:
    #     file = open('QA_Log_{}.txt'.format(tdDate.strftime('%Y%m%d')),'a')
    #     file.write('Historical entity information has not updated, please have a check. \n')
    #     file.close()
    
    # Identify new entities on tdDate from mappingTable
    duplicatedEntity = histEntityInfo.loc[histEntityInfo["CompanyCode"].duplicated(),"CompanyCode"].unique().tolist()
    mappingTable = histEntityInfo.loc[~(histEntityInfo["CompanyCode"].isin(duplicatedEntity)),:].copy() 
    mappingTable = mappingTable.loc[(mappingTable["UpdateDate"]==tdDate),:]  # Identify new entities on tdDate

    # Identify missing entities (have PD yesterday but not today)
    EtyWithPDTdy = pdAll["CompanyCode"].unique().tolist()
    EtyWithPDYstd = activeDates.loc[activeDates["DataDate"]==tdDate-relativedelta(days=1),"CompanyCode"].unique().tolist()
    MissingEty = [x for x in EtyWithPDYstd if x not in EtyWithPDTdy]
    NewEty = [x for x in EtyWithPDTdy if x not in EtyWithPDYstd]
    
    
    # Compare New Entity with historical mappingTable
    if len(NewEty)>0:
        New = pd.DataFrame({"CompanyCode":NewEty})
        New.loc[New["CompanyCode"].isin(mappingTable["CompanyCode"].unique().tolist()),"Flag"] = 1 # New and Recorded in mappingTable
        New.loc[~(New["CompanyCode"].isin(mappingTable["CompanyCode"].unique().tolist())),"Flag"] = 0 # New but not Recorded in mappingTable
        New["Status"] = "New"
    else:
        New = pd.DataFrame(columns=['CompanyCode','Flag','Status'])

    
    # Compare Missing Entity with EtyWithRFnoPD
    EtyWithRFnoPD = EtyWithRFnoPD[EtyWithRFnoPD['CntMissingRF']>0]
    if len(MissingEty)>0:
        Missing = pd.DataFrame({"CompanyCode":MissingEty})
        Missing.loc[Missing["CompanyCode"].isin(EtyWithRFnoPD["CompanyCode"].unique().tolist()),"Flag"] = 1 # Missing due to incomplete RF
        Missing.loc[~(Missing["CompanyCode"].isin(EtyWithRFnoPD["CompanyCode"].unique().tolist())),"Flag"] = 0 # Missing due to unknown reasons
        Missing["Status"] = "Missing"  
    else:
        Missing = pd.DataFrame(columns=['CompanyCode','Flag','Status'])
    
    # Output 1 :Combine New and Missing
    MissingAndNew = pd.concat([Missing,New],ignore_index=True,sort=False)
    tdMappingTable = histEntityInfo.copy()
    tdMappingTable.sort_values(by=["CompanyCode","UpdateDate"],inplace=True)
    tdMappingTable = tdMappingTable.drop_duplicates(["CompanyCode"], keep='last').reset_index(drop=True)
    MissingAndNew = pd.merge(MissingAndNew,tdMappingTable[['CompanyCode','CompanyName','REGION_Name','INDUSTRY_Name']],how='left',on='CompanyCode')
    MissingAndNew.sort_values(by=['Status','Flag'],inplace=True)
    
    # Output 2: Generate comparison of active umbers yesterday and today
    CmprActiveNum = pd.DataFrame(data=[["Global","All",tdDate-relativedelta(days=1),len(EtyWithPDYstd),tdDate,len(EtyWithPDTdy)]],
                                 columns=["REGION_Name","INDUSTRY_Name","Yesterday","YtdActiveNumber","Today","TdActiveNumber"])
    CmprActiveNum['ChangeInNumber'] = abs(CmprActiveNum['TdActiveNumber']-CmprActiveNum['YtdActiveNumber'])
    CmprActiveNum['ChangeInPct'] = CmprActiveNum['ChangeInNumber']/CmprActiveNum['YtdActiveNumber']
    
    return MissingAndNew, CmprActiveNum

# MissingAndNew.to_csv("./Report/{}/GlobalActiveNumDetails.csv".format(tdDate.strftime("%Y%m%d")),index=False)
############################################################################################################################################

############################################################################################################################################
# activeDates = pd.read_csv("D:/Budi/iRAP/Oct 2019 onwards/Product_Coordinator/QA_2021/20210112_Dena/iRAP_Calculation/datesAll.csv")

# Concatenate all PD data calculated by iRAP
# pathTdyPD = "D:/Budi/iRAP/Oct 2019 onwards/Product_Coordinator/QA_2021/PDdataFromCriAT/"
# pdAll = pd.DataFrame([])
# for region in ["IRAP_PUBLIC_AD","IRAP_PUBLIC_CN","IRAP_PUBLIC_EM","IRAP_PUBLIC_EU","IRAP_PUBLIC_IN","IRAP_PUBLIC_NA"]:
#     pdPath = pathTdyPD +  str(region) + "/" + "PD_Individual/"
#     for pdFile in os.listdir(pdPath):
#         pdtemp = pd.read_csv(pdPath + pdFile)
#         pdAll = pdAll.append(pdtemp,ignore_index=True)
# pdAll["DataDate"] = pd.to_datetime(pdAll["DataDate"])

# histEntityInfo = pd.read_csv("D:/Budi/iRAP/Oct 2019 onwards/Product_Coordinator/QA_2021/20210112_Dena/EntityInfoAll.csv")
# EtyWithRFnoPD = pd.read_csv("D:/Budi/iRAP/Oct 2019 onwards/Product_Coordinator/QA_2021/EtyWithRFnoPD_20210106.csv")

# Preprocessing Data for testing purpose

################################################################################################
##                                    IMPORTANT!!                                             ##
## activeDates has updated in function update_regular_individual_data in analytics_process.py ##
##           tdMappingTable and YstdmappingTable can be replaced by self.EntityInfo           ##
################################################################################################

def compare_coverage_daily_PD_sector(activeDates,pdAll,histEntityInfo,tdDate):
    '''
    Parameters
    ----------
    activeDates: pd.DataFrame
        DESCRIPTION: pd.DataFrame of entities with different date records with PD generated 
        COLUMNS: CompanyCode, DataDate
        EXAMPLE: datesAll.csv
        
    pdAll: pd.DataFrame
        DESCRIPTION: as-of-date individual PD of all entities
        with columns ['CompanyCode','DataDate','Horizon','ForwardPoint','PD']
        
    histEntityInfo : pd.DataFrame
        DESCRIPTION: pd.DataFrame of historical mappingTable (to be used to compare with new entities) - as of today
        COLUMNS: CompanyCode, INDUSTRY (optional), REGION (optional), Database (optional), CompanyName (Optional), REGION_Name (Optional), INDUSTRY_Name (Optional), UpdateDate (Good to have), LastDate
        EXAMPLE: EntityInfoAll.csv
    
    tdDate: datetime
        DESCRIPTION: Specify today's date
        EXAMPLE: datetitme.datetime(2021,1,6)

    Returns
    -------
    activeNumber: pd.DataFrame  
        DESCRIPTION: sectors with yesterday and today's number of active entities (also includes absolute and percentage change)
        COLUMNS:
                1. REGION_Name (e.g. Italy)
                2. INDUSTRY_Name (e.g. Financial)
                3. Yesterday: 1 day before tdDate (yesterday)
                4. YtdActiveNumber: number of active entities yesterday
                5. Today: tdDate (today)
                6. TdActiveNumber: number of active entitites today
                7. ChangeInNumber: Absolute change in number of active entities
                8. ChangeInPct: Percentage change in number of active entities

    regionSectorToCheck: pd.DataFrame
        DESCRIPTION: region and sector that has suspicious change in entities number and needs to be check more details on individual member entities 
        COLUMNS: REGITON_Name, INDUSTRY_Name
                
    activeDates: pd.DataFrame 
        DESCRIPTION: updated activeDates in input with entities with latest PD (today)
        COLUMNS: CompanyCode, DataDate
        EXAMPLE: datesAll.csv


    '''          
    
    histEntityInfo["UpdateDate"] = pd.to_datetime(histEntityInfo["UpdateDate"])
    histEntityInfo["LastDate"] = pd.to_datetime(histEntityInfo["LastDate"])
    histEntityInfo = histEntityInfo[histEntityInfo["UpdateDate"]<=tdDate]
    activeDates["DataDate"] = pd.to_datetime(activeDates["DataDate"])
    activeDates = activeDates[activeDates["DataDate"]<tdDate]
    
    # Yesterday entity information
    ytdMappingTable = histEntityInfo[histEntityInfo["UpdateDate"]<tdDate].copy()
    ytdMappingTable.sort_values(by=["CompanyCode","UpdateDate"],inplace=True)
    ytdMappingTable = ytdMappingTable.drop_duplicates(["CompanyCode"], keep='last').reset_index(drop=True)
    
    # Today entity information
    tdMappingTable = histEntityInfo.copy()
    tdMappingTable.sort_values(by=["CompanyCode","UpdateDate"],inplace=True)
    tdMappingTable = tdMappingTable.drop_duplicates(["CompanyCode"], keep='last').reset_index(drop=True)
    
    ### Output 1: Active number report (existing) [Filtered Version]
    ytdActive = activeDates.loc[activeDates["DataDate"]==tdDate-relativedelta(days=1),:]
    ytdActive = pd.merge(ytdActive,ytdMappingTable)
    ytdActiveNum= ytdActive.groupby(["REGION_Name","INDUSTRY_Name","DataDate"])["CompanyCode"]\
                        .count().reset_index().rename(columns={"CompanyCode":"YtdActiveNumber","DataDate":"Yesterday"})
                        
    pdAll["DataDate"] = pd.to_datetime(pdAll["DataDate"])
    pdAll = pdAll[(pdAll["Horizon"]=="12M")&(pdAll["ForwardPoint"]==0)]
    pdAll = pdAll[(~pdAll["PD"].isnull())&(pdAll["DataDate"]==tdDate)]
    # if pdAll.empty:
    #     file = open("QA_Log_{}.txt".format(tdDate.strftime("%Y%m%d")),"a")
    #     file.write("There is no as-of-date individual PD data. \n")
    #     file.close()
    #     return
    tdActive = pdAll.copy()
    tdActive = pd.merge(tdActive,tdMappingTable)
    tdActiveNum = tdActive.groupby(["REGION_Name","INDUSTRY_Name","DataDate"])["CompanyCode"]\
                         .count().reset_index().rename(columns={"CompanyCode":"TdActiveNumber","DataDate":"Today"})

    activeNumber = pd.merge(ytdActiveNum,tdActiveNum)
    activeNumber["ChangeInNumber"] = abs(activeNumber["TdActiveNumber"] - activeNumber["YtdActiveNumber"])
    activeNumber["ChangeInPct"] = activeNumber["ChangeInNumber"]/activeNumber["YtdActiveNumber"]
    activeNumber.sort_values(by=["ChangeInPct","ChangeInNumber","YtdActiveNumber"],ascending=[False,False,False],inplace=True)
    
    ### Output 2: List of region-industry needing more checks
    # SET CRITERIA : 1."ChangeInNumber" is the largest and filter out sector 2."ChangeInPct" is the largest and filter out sector 3."ChangeInPct" is larger than a criteria and number in the sector is larger than 100
    EtyListCriteria1 = (activeNumber["ChangeInNumber"]==activeNumber["ChangeInNumber"].max())
    EtyListCriteria2 = (activeNumber["ChangeInPct"]==activeNumber["ChangeInPct"].max())
    EtyListCriteria3 = (activeNumber["ChangeInPct"]>=0.1)&(activeNumber["YtdActiveNumber"]>=100)
    
    
    regionSectorToCheck = activeNumber.loc[EtyListCriteria1|EtyListCriteria2|EtyListCriteria3,["REGION_Name","INDUSTRY_Name"]].drop_duplicates().reset_index(drop=True)
        
    ### Output 3: Updated datesAll
    if activeDates['DataDate'].max()<tdDate:
        lad = tdActive[["CompanyCode","DataDate"]]
        lad["DataDate"] = pd.to_datetime(lad["DataDate"])
        activeDates = pd.concat([activeDates,lad])
        activeDates = activeDates.loc[activeDates["DataDate"]>=tdDate-relativedelta(months=3),:]
    # else:
    #     file = open("QA_Log_{}.txt".format(tdDate.strftime("%Y%m%d")),"a")
    #     file.write("There is already as-of-date individual PD date in datesAll. \n")
    #     file.close()
        
    activeDates = activeDates.groupby(["CompanyCode","DataDate"]).tail(1)
    activeDates = activeDates.sort_values(by=["CompanyCode","DataDate"],ascending=[True,True])
         
    return activeNumber, regionSectorToCheck, activeDates
############################################################################################################################################

############################################################################################################################################
def compare_daily_entities_with_PD(regionSectorToCheck,activeDates,histEntityInfo,EtyWithRFnoPD,tdDate):
    
    '''
    Parameters
    ----------
    regionSectorToCheck: pd.DataFrame
        DESCRIPTION: region and sector that has suspicious change in entities number and needs to be check more details on individual member entities 
        COLUMNS: REGITON_Name, INDUSTRY_Name
    
    activeDates: pd.DataFrame
        DESCRIPTION: pd.DataFrame of entities with different date records with PD generated 
        COLUMNS: CompanyCode, DataDate
        EXAMPLE: datesAll.csv
        
    histEntityInfo : pd.DataFrame
        DESCRIPTION: pd.DataFrame of historical mappingTable (to be used to compare with new entities) - as of today
        COLUMNS: CompanyCode, INDUSTRY (optional), REGION (optional), Database (optional), CompanyName (Optional), REGION_Name (Optional), INDUSTRY_Name (Optional), UpdateDate (Good to have), LastDate
        EXAMPLE: EntityInfoAll.csv
            
    EtyWithRFnoPD: pd.DataFrame
        DESCRIPTION: pd.DataFrame of entities with risk factors data but no PD (output from getEntitiesWithRFbutNoPD.py)
        COLUMNS: CompanyCode, CntMissingRF
        EXAMPLE: Output of getEntitiesWithRFbutNoPD
    
    tdDate: datetime
        DESCRIPTION: Specify today's date
        EXAMPLE: datetitme.datetime(2021,1,6)

    Returns
    -------
    MissingAndNewAllSectors : pd.DataFrame
        DESCRIPTION: information change of individual member entities of region-industry
        COLUMNS:
                1. CompanyCode: IDBB of companies with suffix "IRAP_ENTITY_" (e.g. IRAP_ENTITY_12345)
                2. INDUSTRY_Name (e.g. Financial)
                3. REGION_Name (e.g. Italy)
                4. Flag: Flag: 0 (New entities and recorded in mappingTable [New Entities] or Missing due to incomplete RF [Missing Entities]), 
                    1 (New entities but not recorded in mappingTable [New Entities]  or Missing due to unknown reasons [Missing Entities]),
                    2 (Only for missing entities: Missing due to change in information )
                5. Status: "New" for new entities in today's region-industry or "Missing" for missing entities in today's region-industry
        
    '''

    
    histEntityInfo["UpdateDate"] = pd.to_datetime(histEntityInfo["UpdateDate"])
    histEntityInfo["LastDate"] = pd.to_datetime(histEntityInfo["LastDate"])
    histEntityInfo = histEntityInfo[histEntityInfo["UpdateDate"]<=tdDate]
    activeDates["DataDate"] = pd.to_datetime(activeDates["DataDate"])
    
    ytdMappingTable = histEntityInfo[histEntityInfo["UpdateDate"]<tdDate].copy()
    ytdMappingTable.sort_values(by=["CompanyCode","UpdateDate"],inplace=True)
    ytdMappingTable = ytdMappingTable.drop_duplicates(["CompanyCode"], keep='last').reset_index(drop=True)
    
    # Today entity information
    tdMappingTable = histEntityInfo.copy()
    tdMappingTable.sort_values(by=["CompanyCode","UpdateDate"],inplace=True)
    tdMappingTable = tdMappingTable.drop_duplicates(["CompanyCode"], keep='last').reset_index(drop=True)
    
    ### Output 1: Active number report (existing) [Filtered Version]
    ytdActive = activeDates.loc[activeDates["DataDate"]==tdDate-relativedelta(days=1),:]
    # if ytdActive.empty:
    #     file = open("QA_Log_{}.txt".format(tdDate.strftime("%Y%m%d")),"a")
    #     file.write("Please check whether input the PD data on {}. \n".format((tdDate-relativedelta(days=1)).strftime("%Y%m%d")))
    #     file.close()  
    #     return
    ytdActive = pd.merge(ytdActive,ytdMappingTable)
                        
    tdActive = activeDates.loc[activeDates["DataDate"]==tdDate,:]
    # if tdActive.empty:
    #     file = open("QA_Log_{}.txt".format(tdDate.strftime("%Y%m%d")),"a")
    #     file.write("Please check whether input the PD data on {}. \n".format(tdDate.strftime("%Y%m%d")))
    #     file.close() 
    #     return
    tdActive = pd.merge(tdActive,tdMappingTable)
    
    # Identify new coming entities on tdDate from mappingTable
    duplicatedEntity = histEntityInfo.loc[histEntityInfo["CompanyCode"].duplicated(),"CompanyCode"].unique().tolist()
    newComingEntityInfo = histEntityInfo.loc[~(histEntityInfo["CompanyCode"].isin(duplicatedEntity)),:].copy() 
    newComingEntityInfo = newComingEntityInfo.loc[(newComingEntityInfo["UpdateDate"]==tdDate),:]  # Identify new coming entities on tdDate
    MissingAndNewAllSectors = pd.DataFrame([])
    regionSectorToCheck = regionSectorToCheck.reset_index(drop=True)
    for i in regionSectorToCheck.index.tolist():
        region = regionSectorToCheck.loc[i,"REGION_Name"]
        industry = regionSectorToCheck.loc[i,"INDUSTRY_Name"]
        
        ytdCompany_Sector = ytdActive[["CompanyCode","DataDate","CompanyName","REGION_Name","INDUSTRY_Name"]].copy()
        ytdCompany_Sector = ytdCompany_Sector.loc[(ytdCompany_Sector["REGION_Name"]==region)&(ytdCompany_Sector["INDUSTRY_Name"]==industry),:]
        
        tdCompany_Sector = tdActive[["CompanyCode","DataDate","CompanyName","REGION_Name","INDUSTRY_Name"]].copy()
        tdCompany_Sector = tdCompany_Sector.loc[(tdCompany_Sector["REGION_Name"]==region)&(tdCompany_Sector["INDUSTRY_Name"]==industry),:]
        
        EtyWithPDTd = tdCompany_Sector["CompanyCode"].unique().tolist()
        EtyWithPDYtd = ytdCompany_Sector["CompanyCode"].unique().tolist()
        
        # Identify missing entities (have PD yesterday but not today)
        MissingEty = [x for x in EtyWithPDYtd if x not in EtyWithPDTd]
        
        # Identify new entities (have PD today, no PD yesterday)
        NewEty = [x for x in EtyWithPDTd if x not in EtyWithPDYtd]
        
        # Compare New Entity with historical mappingTable
        
        if len(NewEty)>0:
            New = pd.DataFrame({"CompanyCode":NewEty})
            New.loc[New["CompanyCode"].isin(newComingEntityInfo["CompanyCode"].unique().tolist()),"Flag"] = 1 # New and Recorded in mappingTable_temp
            New.loc[~(New["CompanyCode"].isin(newComingEntityInfo["CompanyCode"].unique().tolist())),"Flag"] = 0 # New but not Recorded in mappingTable_temp
            New["Status"] = "New"
            New = pd.merge(New,tdCompany_Sector[["CompanyCode","CompanyName"]],how="left",on="CompanyCode")
        else:
            New = pd.DataFrame(columns=["CompanyCode","Flag","Status","CompanyName"])
        
        # Compare Missing Entity with EtyWithRFnoPD
        EtyWithRFnoPD = EtyWithRFnoPD[EtyWithRFnoPD['CntMissingRF']>0]
        if len(MissingEty)>0:
            Missing = pd.DataFrame({"CompanyCode":MissingEty})
            Missing.loc[Missing["CompanyCode"].isin(EtyWithRFnoPD["CompanyCode"].unique().tolist()),"Flag"] = 1 # Missing due to incomplete RF
            Missing.loc[~(Missing["CompanyCode"].isin(EtyWithRFnoPD["CompanyCode"].unique().tolist())),"Flag"] = 0 # Missing due to unknown reasons
            Missing["Status"] = "Missing"
            Missing = pd.merge(Missing,ytdCompany_Sector[["CompanyCode","CompanyName"]],how="left",on="CompanyCode")
        else:
            Missing = pd.DataFrame(columns=["CompanyCode","Flag","Status","CompanyName"])
        
        # Check missing entity or 
        # Combine New and Missing
        MissingAndNew = pd.concat([Missing,New],ignore_index=True,sort=False)
        MissingAndNew["REGION_Name"] = region
        MissingAndNew["INDUSTRY_Name"] = industry
        MissingAndNew = MissingAndNew[["CompanyCode","Flag","Status","CompanyName","REGION_Name","INDUSTRY_Name"]]
        
        MissingAndNewAllSectors = pd.concat([MissingAndNewAllSectors,MissingAndNew])
        
    # New changed information for existing entities
    newChangeEntityInfo = histEntityInfo.loc[histEntityInfo["CompanyCode"].isin(duplicatedEntity),:].copy()
    cols = ['CompanyCode','INDUSTRY','REGION']    
    newChangeEntityInfo = newChangeEntityInfo.drop_duplicates(cols, keep=False).reset_index(drop=True)
    newChangeEntityInfo = newChangeEntityInfo.loc[(newChangeEntityInfo["UpdateDate"]==tdDate),:]  # Identify new change entities on tdDate
    if len(newChangeEntityInfo)>0:
        idxOfChgInfo = (MissingAndNewAllSectors["CompanyCode"].isin(newChangeEntityInfo["CompanyCode"]))&(MissingAndNewAllSectors["Flag"]==0) # new comming or missing due to information change
        MissingAndNewAllSectors.loc[idxOfChgInfo,"Flag"] = 2 # Missing due to change in information
        
    MissingAndNewAllSectors.sort_values(by=["REGION_Name","INDUSTRY_Name","Status","Flag"])        
    return MissingAndNewAllSectors
         
