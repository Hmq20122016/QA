# -*- coding: utf-8 -*-
"""
Created on Thu Feb 11 10:32:19 2021

@author: Kou Dena
"""

import pandas as pd
from dateutil.relativedelta import relativedelta
import matplotlib.pyplot as plt
import os

def check_folder(path):
	if	not os.path.exists(path):
		os.makedirs(path)
		print('^_^ Creat folder {} ^_^'.format(path))

############################################################################################################################################

def set_threshold():
    threshold = {
        'indNum': 0.01,
        'indPct': 0.5,
        'aggNum': 0.001,
        'aggPct': 0.3
        }
    return threshold
    
# sectorMappingTable is a static table
def get_agg_PD_change(perAggPD, aggPD, sectorMappingTable, tdDate):
    '''
    Parameters
    ----------
    perAggPD : DataFrame
        DESCRIPTION: read from 30days_aggPD.csv
        with columns ['ObjectID','DataDate','PD']
    aggPD : DataFrame
        DESCRIPTION: today's aggregate PD
        with columns ['ObjectID','DataDate','Horizon','ForwardPoint','AggregateType','PD']
    sectorMappingTable : DataFrame
        DESCRIPTION: read from sectorMappingTable.csv which describes which region and which industry of each ObjectID
        with columns ['ObjectID','REGION_Name','INDUSTRY_Name']
    tdDate : datetime
        DESCRIPTION: as-of-date of PD

    Returns
    -------
    pdReport : DataFrame
        DESCRIPTION: suspicious jump in aggregate PD
                     1. aggregate type: mean
                     2. suspicious: ChangeInPct>0.3 & ChangeInNumber>0.001
                     3. ChangeInPct: abs(tdAggPD-meanOfPast30DayAggPD)/meanOfPast30DayAggPD
                     4. ChangeInNum: abs(tdAggPD-ytdAggPD)
        with columns ['ObjectID','REGION_Name','INDUSTRY_Name','DataDate','PD','ytdPD','Mean','ChangeInNumber','ChangeInPct']
    perAggPD : DataFrame
        DESCRIPTION: updated last 30-day aggregate(mean) PD from as-of-date
        with columns ['ObjectID','DataDate','PD']
    '''  
    perAggPD['DataDate'] = pd.to_datetime(perAggPD['DataDate'])
    aggPD['DataDate'] = pd.to_datetime(aggPD['DataDate'])
    aggPDMean = perAggPD[perAggPD['DataDate']<tdDate].groupby('ObjectID')['PD'].mean().reset_index().rename(columns={'PD':'Mean'})
    tdAggPD = aggPD.loc[(aggPD['DataDate']==tdDate)&(aggPD['Horizon']=='12M')&(aggPD['ForwardPoint']==0)&(aggPD['AggregateType']=='MEAN'),['ObjectID','DataDate','PD']].copy()
    # if tdAggPD.empty:
    #     file = open("QA_Log_{}.txt".format(tdDate.strftime("%Y%m%d")),"a")
    #     file.write("There is no as-of-date aggregate PD data. \n")
    #     file.close()
    #     return
    ytdAggPD = perAggPD.loc[perAggPD['DataDate'] == tdDate-relativedelta(days=1),['ObjectID','PD']].rename(columns={'PD':'ytdPD'})
    # if ytdAggPD.empty:
    #     file = open("QA_Log_{}.txt".format(tdDate.strftime("%Y%m%d")),"a")
    #     file.write("There is no last day's aggregate PD data. \n")
    #     file.close()
    #     return
    tmpPD = pd.merge(aggPDMean,tdAggPD,on='ObjectID')
    tmpPD = pd.merge(tmpPD,ytdAggPD,on='ObjectID') 
    tmpPD['ChangeInNumber'] = abs(tmpPD['PD']-tmpPD['ytdPD'])
    tmpPD['ChangeInPct'] = abs(tmpPD['PD']-tmpPD['Mean'])/tmpPD['Mean']
    #pdReport = tmpPD.loc[(tmpPD.ChangeInPct >0.5)&(tmpPD.ChangeInNumber>0.01),['ObjectID','DataDate','PD','ytdPD','Mean','ChangeInNumber','ChangeInPct']]
    threshold = set_threshold()
    pdReport = tmpPD.loc[(tmpPD.ChangeInPct >threshold['aggPct'])&(tmpPD.ChangeInNumber>threshold['aggNum']),['ObjectID','DataDate','PD','ytdPD','Mean','ChangeInNumber','ChangeInPct']]
    pdReport = pd.merge(pdReport, sectorMappingTable)[['ObjectID','REGION_Name','INDUSTRY_Name','DataDate','PD','ytdPD','Mean','ChangeInNumber','ChangeInPct']]
    if perAggPD['DataDate'].max()<tdDate:
        perAggPD = pd.concat([perAggPD,tdAggPD])
        perAggPD = perAggPD.groupby(['ObjectID','DataDate']).tail(1)
        perAggPD = perAggPD.loc[perAggPD['DataDate']>tdDate-relativedelta(days=31),:]
    # else:
    #     file = open("QA_Log_{}.txt".format(tdDate.strftime("%Y%m%d")),"a")
    #     file.write("There is already as-of-date aggregate PD data in 30days_aggPD. \n")
    #     file.close()
    perAggPD.sort_values(by=['ObjectID','DataDate'],inplace=True)
    return pdReport, perAggPD

# perAggPD.to_csv('30days_aggPD.csv',index=False)
# check_folder('./Report/{}'.format(tdDate))
# pdReport.to_csv('./Report/{}/aggPD_Change.csv'.format(tdDate),index=False)

def get_agg_PD_change_BICS2020(perAggPD, aggPD, sectorMappingTableBICS2020, tdDate):
    '''
    Parameters
    ----------
    perAggPD : DataFrame
        DESCRIPTION: read from 30days_aggPD.csv
        with columns ['ObjectID','DataDate','PD']
    aggPD : DataFrame
        DESCRIPTION: today's aggregate PD
        with columns ['ObjectID','DataDate','Horizon','ForwardPoint','AggregateType','PD']
    sectorMappingTableBICS2020 : DataFrame
        DESCRIPTION: read from sectorMappingTable_BICS2020.csv which describes which region and which industry of each ObjectID
        with columns ['ObjectID', 'REGION_Name', 'INDUSTRY_LEVEL_1_Name','INDUSTRY_LEVEL_2_Name']
    tdDate : datetime
        DESCRIPTION: as-of-date of PD

    Returns
    -------
    pdReport : DataFrame
        DESCRIPTION: suspicious jump in aggregate PD
                     1. aggregate type: mean
                     2. suspicious: ChangeInPct>0.3 & ChangeInNumber>0.001
                     3. ChangeInPct: abs(tdAggPD-meanOfPast30DayAggPD)/meanOfPast30DayAggPD
                     4. ChangeInNum: abs(tdAggPD-ytdAggPD)
        with columns ['ObjectID','REGION_Name','INDUSTRY_LEVEL_1_Name','INDUSTRY_LEVEL_2_Name','DataDate','PD','ytdPD','Mean','ChangeInNumber','ChangeInPct']
    perAggPD : DataFrame
        DESCRIPTION: updated last 30-day aggregate(mean) PD from as-of-date
        with columns ['ObjectID','DataDate','PD']
    '''  
    perAggPD['DataDate'] = pd.to_datetime(perAggPD['DataDate'])
    aggPD['DataDate'] = pd.to_datetime(aggPD['DataDate'])
    aggPDMean = perAggPD[perAggPD['DataDate']<tdDate].groupby('ObjectID')['PD'].mean().reset_index().rename(columns={'PD':'Mean'})
    tdAggPD = aggPD.loc[(aggPD['DataDate']==tdDate)&(aggPD['Horizon']=='12M')&(aggPD['ForwardPoint']==0)&(aggPD['AggregateType']=='MEAN'),['ObjectID','DataDate','PD']].copy()
    # if tdAggPD.empty:
    #     file = open("QA_Log_{}.txt".format(tdDate.strftime("%Y%m%d")),"a")
    #     file.write("There is no as-of-date aggregate PD data. \n")
    #     file.close()
    #     return
    ytdAggPD = perAggPD.loc[perAggPD['DataDate'] == tdDate-relativedelta(days=1),['ObjectID','PD']].rename(columns={'PD':'ytdPD'})
    # if ytdAggPD.empty:
    #     file = open("QA_Log_{}.txt".format(tdDate.strftime("%Y%m%d")),"a")
    #     file.write("There is no last day's aggregate PD data. \n")
    #     file.close()
    #     return
    tmpPD = pd.merge(aggPDMean,tdAggPD,on='ObjectID')
    tmpPD = pd.merge(tmpPD,ytdAggPD,on='ObjectID') 
    tmpPD['ChangeInNumber'] = abs(tmpPD['PD']-tmpPD['ytdPD'])
    tmpPD['ChangeInPct'] = abs(tmpPD['PD']-tmpPD['Mean'])/tmpPD['Mean']
    #pdReport = tmpPD.loc[(tmpPD.ChangeInPct >0.5)&(tmpPD.ChangeInNumber>0.01),['ObjectID','DataDate','PD','ytdPD','Mean','ChangeInNumber','ChangeInPct']]
    threshold = set_threshold()
    pdReport = tmpPD.loc[(tmpPD.ChangeInPct >threshold['aggPct'])&(tmpPD.ChangeInNumber>threshold['aggNum']),['ObjectID','DataDate','PD','ytdPD','Mean','ChangeInNumber','ChangeInPct']]
    pdReport = pd.merge(pdReport, sectorMappingTableBICS2020)[['ObjectID','REGION_Name','INDUSTRY_LEVEL_1_Name','INDUSTRY_LEVEL_2_Name',
                                                               'DataDate','PD','ytdPD','Mean','ChangeInNumber','ChangeInPct']]
    if perAggPD['DataDate'].max()<tdDate:
        perAggPD = pd.concat([perAggPD,tdAggPD])
        perAggPD = perAggPD.groupby(['ObjectID','DataDate']).tail(1)
        perAggPD = perAggPD.loc[perAggPD['DataDate']>tdDate-relativedelta(days=31),:]
    # else:
    #     file = open("QA_Log_{}.txt".format(tdDate.strftime("%Y%m%d")),"a")
    #     file.write("There is already as-of-date aggregate PD data in 30days_aggPD. \n")
    #     file.close()
    perAggPD.sort_values(by=['ObjectID','DataDate'],inplace=True)
    perAggPD['DataDate'] = perAggPD['DataDate'].dt.date
    return pdReport, perAggPD
############################################################################################################################################

############################################################################################################################################
def get_individual_PD_change(perPD, pdAll, histEntityInfo, tdDate, regionSectorToCheck=pd.DataFrame([])):
    
    '''
    
    Parameters
    ----------
    perPD : DataFrame
        DESCRIPTION: read from 30days_PD.csv
        with columns ['CompanyCode','DataDate','PD']
    pdAll : DataFrame
        DESCRIPTION: today's individual PD
        with columns ['CompanyCode','DataDate','Horizon','ForwardPoint','PD']
    histEntityInfo : DataFrame
        DESCRIPTION: read from EntityInfoAll.csv
        with columns ['CompanyCode','INDUSTRY','REGION','Database','CompanyName','REGION_Name','INDUSTRY_Name','UpdateDate','LastDate']
    tdDate : datetime
        DESCRIPTION: as-of-date of PD
    regionSectorToCheck : DataFrame, optional
        DESCRIPTION: region and sector list need to be checked more in individual PD change. The default is pd.DataFrame([]).
        with columns ['REGION_Name','INDUSTRY_Name']

    Returns
    -------
    pdReport : DataFrame
        DESCRIPTION: if regionSectorToCheck is empty, suspicious jump in individual PD
                        1. suspicious: ChangeInPct>0.5 & ChangeInNumber>0.01
                        2. ChangeInPct: abs(tdPD-meanOfPast30DayPD)/meanOfPast30DayPD
                        3. ChangeInNum: abs(tdPD-ytdPD)
                     if regionSectorToCheck is not empty, all individual PD of the region-sector with their ChangeInPct and ChangeInNum 
        with columns ['CompanyCode','CompanyName','DataDate','REGION_Name','INDUSTRY_Name','PD','ytdPD','Mean','ChangeInNumber','ChangeInPct']
    perPD : DataFrame
        DESCRIPTION: updated last 30-day PD from as-of-date
        with columns ['CompanyCode','DataDate','PD']

    '''
    
    histEntityInfo["UpdateDate"] = pd.to_datetime(histEntityInfo["UpdateDate"])
    histEntityInfo["LastDate"] = pd.to_datetime(histEntityInfo["LastDate"])
    # Today entity information
    tdMappingTable = histEntityInfo[histEntityInfo["UpdateDate"]<=tdDate].copy()
    tdMappingTable.sort_values(by=["CompanyCode","UpdateDate"],inplace=True)
    tdMappingTable = tdMappingTable.drop_duplicates(["CompanyCode"], keep='last').reset_index(drop=True)
    
    perPD['DataDate'] = pd.to_datetime(perPD['DataDate'])
    pdAll['DataDate'] = pd.to_datetime(pdAll['DataDate'])
    pdMean = perPD[perPD['DataDate']<tdDate].groupby('CompanyCode')['PD'].mean().reset_index().rename(columns={'PD':'Mean'})
    tdPD = pdAll.loc[(pdAll['DataDate']==tdDate)&(pdAll['Horizon']=='12M')&(pdAll['ForwardPoint']==0),['CompanyCode','DataDate','PD']].copy()
    # if tdPD.empty:
    #     file = open("QA_Log_{}.txt".format(tdDate.strftime("%Y%m%d")),"a")
    #     file.write("There is no as-of-date PD data. \n")
    #     file.close()
    #     return
    ytdPD = perPD.loc[perPD['DataDate'] == tdDate-relativedelta(days=1),['CompanyCode','PD']].rename(columns={'PD':'ytdPD'})
    # if ytdPD.empty:
    #     file = open("QA_Log_{}.txt".format(tdDate.strftime("%Y%m%d")),"a")
    #     file.write("There is no last day's PD data. \n")
    #     file.close()
    #     return
    tmpPD = pd.merge(pdMean,tdPD)
    tmpPD = pd.merge(tmpPD,ytdPD)
    tmpPD['ChangeInNumber'] = abs(tmpPD['PD']-tmpPD['ytdPD'])
    tmpPD['ChangeInPct'] = abs(tmpPD['PD']-tmpPD['Mean'])/tmpPD['Mean']
    tmpPD = pd.merge(tmpPD,tdMappingTable)
    threshold = set_threshold()
    if not regionSectorToCheck.empty:
        pdReport = pd.merge(tmpPD,regionSectorToCheck,how='inner',on=['REGION_Name','INDUSTRY_Name'])
        pdReport.sort_values(by=['REGION_Name','INDUSTRY_Name','ChangeInPct','ChangeInNumber'],inplace=True)
    elif len(regionSectorToCheck):
        pdReport = pd.DataFrame(columns=['CompanyCode','CompanyName','DataDate','REGION_Name','INDUSTRY_Name','PD','ytdPD','Mean','ChangeInNumber','ChangeInPct'])
    else:
        pdReport = tmpPD.loc[(tmpPD.ChangeInPct >threshold['indPct'])&(tmpPD.ChangeInNumber>threshold['indNum']),:]
        pdReport.sort_values(by=['ChangeInPct','ChangeInNumber'],inplace=True)
        
    pdReport = pdReport[['CompanyCode','CompanyName','DataDate','REGION_Name','INDUSTRY_Name','PD','ytdPD','Mean','ChangeInNumber','ChangeInPct']]
    
    if perPD['DataDate'].max()<tdDate:
        perPD = pd.concat([perPD,tdPD])
        perPD = perPD.groupby(['CompanyCode','DataDate']).tail(1)
        perPD = perPD.loc[perPD['DataDate']>tdDate-relativedelta(days=31),:]
    # else:
    #     file = open("QA_Log_{}.txt".format(tdDate.strftime("%Y%m%d")),"a")
    #     file.write("There is already as-of-date PD data in 30days_aggPD. \n")
    #     file.close()
    
    return pdReport, perPD

# perPD.to_csv('30days_PD.csv',index=False)
# check_folder('./Report/{}'.format(tdDate))
# pdReport.to_csv('./Report/{}/PD_Change.csv'.format(tdDate),index=False)

def get_individual_PD_change_BICS2020(perPD, pdAll, histEntityInfo, tdMappingTableBICS2020, tdDate, regionSectorToCheck=pd.DataFrame([])):
    
    '''
    
    Parameters
    ----------
    perPD : DataFrame
        DESCRIPTION: read from 30days_PD.csv
        with columns ['CompanyCode','DataDate','PD']
    pdAll : DataFrame
        DESCRIPTION: today's individual PD
        with columns ['CompanyCode','DataDate','Horizon','ForwardPoint','PD']
    histEntityInfo : DataFrame
        DESCRIPTION: read from EntityInfoAll.csv
        with columns ['CompanyCode','INDUSTRY','REGION','Database','CompanyName','REGION_Name','INDUSTRY_Name','UpdateDate','LastDate']
    tdMappingTableBICS2020 : DataFrame
        DESCRIPTION: region and industry (BICS2020) information for each company
        with columns ['CompanyCode', 'REGION_Name', 'INDUSTRY_LEVEL_1_Name','INDUSTRY_LEVEL_2_Name']
    tdDate : datetime
        DESCRIPTION: as-of-date of PD
    regionSectorToCheck : DataFrame, optional
        DESCRIPTION: region and sector list need to be checked more in individual PD change. The default is pd.DataFrame([]).
        with columns ['REGION_Name','INDUSTRY_LEVEL_1_Name','INDUSTRY_LEVEL_2_Name']

    Returns
    -------
    pdReport : DataFrame
        DESCRIPTION: if regionSectorToCheck is empty, suspicious jump in individual PD
                        1. suspicious: ChangeInPct>0.5 & ChangeInNumber>0.01
                        2. ChangeInPct: abs(tdPD-meanOfPast30DayPD)/meanOfPast30DayPD
                        3. ChangeInNum: abs(tdPD-ytdPD)
                     if regionSectorToCheck is not empty, all individual PD of the region-sector with their ChangeInPct and ChangeInNum 
        with columns ['CompanyCode','CompanyName','DataDate','REGION_Name','INDUSTRY_LEVEL_1_Name','INDUSTRY_LEVEL_2_Name','PD','ytdPD','Mean','ChangeInNumber','ChangeInPct']
    perPD : DataFrame
        DESCRIPTION: updated last 30-day PD from as-of-date
        with columns ['CompanyCode','DataDate','PD']

    '''
    histEntityInfo["UpdateDate"] = pd.to_datetime(histEntityInfo["UpdateDate"])
    histEntityInfo["LastDate"] = pd.to_datetime(histEntityInfo["LastDate"])
    # Today entity information
    tdMappingTable = histEntityInfo[histEntityInfo["UpdateDate"]<=tdDate].copy()
    tdMappingTable.sort_values(by=["CompanyCode","UpdateDate"],inplace=True)
    tdMappingTable = tdMappingTable.drop_duplicates(["CompanyCode"], keep='last').reset_index(drop=True)
    tdMappingTableBICS2020 = pd.merge(tdMappingTableBICS2020,tdMappingTable[['CompanyCode','CompanyName']],how='left')
    
    perPD['DataDate'] = pd.to_datetime(perPD['DataDate'])
    pdAll['DataDate'] = pd.to_datetime(pdAll['DataDate'])
    pdMean = perPD[perPD['DataDate']<tdDate].groupby('CompanyCode')['PD'].mean().reset_index().rename(columns={'PD':'Mean'})
    tdPD = pdAll.loc[(pdAll['DataDate']==tdDate)&(pdAll['Horizon']=='12M')&(pdAll['ForwardPoint']==0),['CompanyCode','DataDate','PD']].copy()
    # if tdPD.empty:
    #     file = open("QA_Log_{}.txt".format(tdDate.strftime("%Y%m%d")),"a")
    #     file.write("There is no as-of-date PD data. \n")
    #     file.close()
    #     return
    ytdPD = perPD.loc[perPD['DataDate'] == tdDate-relativedelta(days=1),['CompanyCode','PD']].rename(columns={'PD':'ytdPD'})
    # if ytdPD.empty:
    #     file = open("QA_Log_{}.txt".format(tdDate.strftime("%Y%m%d")),"a")
    #     file.write("There is no last day's PD data. \n")
    #     file.close()
    #     return
    tmpPD = pd.merge(pdMean,tdPD)
    tmpPD = pd.merge(tmpPD,ytdPD)
    tmpPD['ChangeInNumber'] = abs(tmpPD['PD']-tmpPD['ytdPD'])
    tmpPD['ChangeInPct'] = abs(tmpPD['PD']-tmpPD['Mean'])/tmpPD['Mean']
    tmpPD = pd.merge(tmpPD,tdMappingTableBICS2020)
    threshold = set_threshold()
    if not regionSectorToCheck.empty:
        regionSector = regionSectorToCheck[(regionSectorToCheck['REGION_Name']!='Global')&(regionSectorToCheck['INDUSTRY_LEVEL_1_Name']!='All')&(regionSectorToCheck['INDUSTRY_LEVEL_2_Name']!='All')]
        pdReport = pd.merge(tmpPD,regionSector,how='inner',on=['REGION_Name','INDUSTRY_LEVEL_1_Name','INDUSTRY_LEVEL_2_Name'])
        
        regionSector = regionSectorToCheck[(regionSectorToCheck['REGION_Name']!='Global')&(regionSectorToCheck['INDUSTRY_LEVEL_1_Name']!='All')&(regionSectorToCheck['INDUSTRY_LEVEL_2_Name']=='All')]
        if not regionSector.empty:
            tmpPdReport = pd.merge(tmpPD,regionSector[['REGION_Name','INDUSTRY_LEVEL_1_Name']],how='inner',on=['REGION_Name','INDUSTRY_LEVEL_1_Name'])
            tmpPdReport = tmpPdReport.loc[(tmpPdReport.ChangeInPct >threshold['aggPct'])&(tmpPdReport.ChangeInNumber>threshold['aggNum'])].copy()
            tmpPdReport[['INDUSTRY_LEVEL_2_Name']] = 'All'
            pdReport = pd.concat([tmpPdReport,pdReport])
            
        regionSector = regionSectorToCheck[(regionSectorToCheck['REGION_Name']!='Global')&(regionSectorToCheck['INDUSTRY_LEVEL_1_Name']=='All')]
        if not regionSector.empty:
            tmpPdReport = pd.merge(tmpPD,regionSector[['REGION_Name']],how='inner',on=['REGION_Name'])
            tmpPdReport = tmpPdReport.loc[(tmpPdReport.ChangeInPct >threshold['aggPct'])&(tmpPdReport.ChangeInNumber>threshold['aggNum']),:].copy()
            tmpPdReport[['INDUSTRY_LEVEL_1_Name','INDUSTRY_LEVEL_2_Name']] = ['All','All']
            pdReport = pd.concat([tmpPdReport,pdReport])
            
        regionSector = regionSectorToCheck[(regionSectorToCheck['REGION_Name']=='Global')&(regionSectorToCheck['INDUSTRY_LEVEL_1_Name']!='All')&(regionSectorToCheck['INDUSTRY_LEVEL_2_Name']!='All')]
        if not regionSector.empty:
            tmpPdReport = pd.merge(tmpPD,regionSector[['INDUSTRY_LEVEL_1_Name','INDUSTRY_LEVEL_2_Name']],how='inner',on=['INDUSTRY_LEVEL_1_Name','INDUSTRY_LEVEL_2_Name'])
            tmpPdReport = tmpPdReport.loc[(tmpPdReport.ChangeInPct >threshold['aggPct'])&(tmpPdReport.ChangeInNumber>threshold['aggNum']),:].copy()
            tmpPdReport[['REGION_Name']] = 'Global'
            pdReport = pd.concat([tmpPdReport,pdReport])
            
        regionSector = regionSectorToCheck[(regionSectorToCheck['REGION_Name']=='Global')&(regionSectorToCheck['INDUSTRY_LEVEL_1_Name']!='All')&(regionSectorToCheck['INDUSTRY_LEVEL_2_Name']=='All')]
        if not regionSector.empty:
            tmpPdReport = pd.merge(tmpPD,regionSector[['INDUSTRY_LEVEL_1_Name']],how='inner',on=['INDUSTRY_LEVEL_1_Name'])
            tmpPdReport = tmpPdReport.loc[(tmpPdReport.ChangeInPct >threshold['aggPct'])&(tmpPdReport.ChangeInNumber>threshold['aggNum']),:].copy()
            tmpPdReport[['REGION_Name','INDUSTRY_LEVEL_2_Name']] = ['Global','All']
            pdReport = pd.concat([tmpPdReport,pdReport])
            
        regionSector = regionSectorToCheck[(regionSectorToCheck['REGION_Name']=='Global')&(regionSectorToCheck['INDUSTRY_LEVEL_1_Name']=='All')]
        if not regionSector.empty:
            tmpPdReport = tmpPD.loc[(tmpPD.ChangeInPct >threshold['aggPct'])&(tmpPD.ChangeInNumber>threshold['aggNum']),:].copy()
            tmpPdReport[['REGION_Name','INDUSTRY_LEVEL_1_Name','INDUSTRY_LEVEL_2_Name']] = ['Global','All','All']
            pdReport = pd.concat([tmpPdReport,pdReport])
            
        pdReport.sort_values(by=['REGION_Name','INDUSTRY_LEVEL_1_Name','INDUSTRY_LEVEL_2_Name','ChangeInPct','ChangeInNumber'],inplace=True)
    elif len(regionSectorToCheck.columns):
        pdReport = pd.DataFrame(columns=['CompanyCode','CompanyName','DataDate','REGION_Name','INDUSTRY_LEVEL_1_Name','INDUSTRY_LEVEL_2_Name','PD','ytdPD','Mean','ChangeInNumber','ChangeInPct'])
    else:
        pdReport = tmpPD.loc[(tmpPD.ChangeInPct >threshold['indPct'])&(tmpPD.ChangeInNumber>threshold['indNum']),:]
        pdReport.sort_values(by=['ChangeInPct','ChangeInNumber'],inplace=True)
        
    pdReport = pdReport[['CompanyCode','CompanyName','DataDate','REGION_Name','INDUSTRY_LEVEL_1_Name','INDUSTRY_LEVEL_2_Name','PD','ytdPD','Mean','ChangeInNumber','ChangeInPct']]
    
    if perPD['DataDate'].max()<tdDate:
        perPD = pd.concat([perPD,tdPD])
        perPD = perPD.groupby(['CompanyCode','DataDate']).tail(1)
        perPD = perPD.loc[perPD['DataDate']>tdDate-relativedelta(days=31),:]
    # else:
    #     file = open("QA_Log_{}.txt".format(tdDate.strftime("%Y%m%d")),"a")
    #     file.write("There is already as-of-date PD data in 30days_aggPD. \n")
    #     file.close()
    
    return pdReport, perPD
    
############################################################################################################################################

############################################################################################################################################
# mdfAll = pd.read_csv(r'D:\Quality Control\QualityControlCriAT\QA_temp\mdf_ind.csv')
# memberInfo = pd.read_csv(r'D:\Quality Control\QualityControlCriAT\QA_temp\PD_Change_20210120.csv')
# memberInfo = memberInfo[['CompanyCode','CompanyName']]
# savePath = './Report/20210106/Individual/' # savePath = './Report/20210106/Aggregate/'

def plot_MDF_of_list(mdfAll, memberInfo, savePath):
   
    '''
    Parameters
    ----------
    mdfAll : DataFrame
        DESCRIPTION: individual MDF or aggregated MDF
        with columns ['CompanyCode','DataDate','VariableID','MDFValue'] for individual 
        or ['ObjectID','DataDate','VariableID','MDFValue'] for aggregated 
    memberInfo : DataFrame
        DESCRIPTION: columns ['CompanyCode','CompanyName'] for individual
        or ['ObjectID','REGION_Name','INDUSTRY_Name'] for aggregated
    savePath : string
        DESCRIPTION: savepath of figures
    Returns
    -------
    None.

    ''' 
    memCol = memberInfo.columns.to_list()
    memID = memCol[0]
    memCol.remove(memID)
    memList = memberInfo[memID].unique().copy()
    for imem in memList:
        plt.close('all')
        fig = plt.figure(figsize=[16,12])
        tmpmdf = mdfAll.loc[mdfAll[memID]==imem,['VariableID','MDFValue']].copy()
        tmpmdf.sort_values(by='MDFValue',inplace=True)
        plt.barh(tmpmdf['VariableID'],tmpmdf['MDFValue'])
        plt.grid(b = True,color ='grey',linestyle ='-.',linewidth = 0.5,alpha = 0.2)
        title = ''
        for icol in memCol:
            title = title + memberInfo.loc[memberInfo[memID]==imem,icol].iloc[0] + '_'
        title = title[:-1]
        plt.title(title,fontsize=20)
        title = title.replace('/','&')
        fig.savefig('{}/{}.png'.format(savePath,title), facecolor=fig.get_facecolor())

############################################################################################################################################

############################################################################################################################################
def get_risk_factors_of_list(perRF, rfAll, memberInfo, tdDate):
    
    '''
    Parameters
    ----------
    perRF : DataFrame
        DESCRIPTION: read from 30days_RF.csv
        with columns ['CompanyCode','DataDate','RFID','RFValue']
    rfAll : DataFrame
        DESCRIPTION: today's risk factors
        with columns ['CompanyCode','DataDate','RFID','RFValue']
    memberInfo : DataFrame
        DESCRIPTION: columns ['CompanyCode','CompanyName'] for individual
    tdDate : datetime
        DESCRIPTION: as-of-date of PD

    Returns
    -------
    rfReport : DataFrame
        DESCRIPTION: today and yesterday's risk factor values of entites in memberInfo
        with columns ['CompanyCode','DataDate','RFID','RFValue','RFPct','ytdRFValue','ytdRFPct']
    perRF : DataFrame
        DESCRIPTION: updated last 30-day risk factors from as-of-date
        with columns ['CompanyCode','DataDate','RFID','RFValue','RFPercentile']

    '''
          
    perRF['DataDate'] = pd.to_datetime(perRF['DataDate'])
    rfAll['DataDate'] = pd.to_datetime(rfAll['DataDate'])
    
    tdRF = rfAll.loc[rfAll['DataDate']==tdDate,['CompanyCode','DataDate','RFID','RFValue']].copy()
    # if tdRF.empty:
    #     file = open("QA_Log_{}.txt".format(tdDate.strftime("%Y%m%d")),"a")
    #     file.write("There is no as-of-date risk factors data. \n")
    #     file.close()
    #     return
    ytdRF = perRF.loc[perRF['DataDate'] == tdDate-relativedelta(days=1),['CompanyCode','RFID','RFValue']].copy().rename(columns={'RFValue':'ytdRFValue'})
    # if ytdRF.empty:
    #     file = open("QA_Log_{}.txt".format(tdDate.strftime("%Y%m%d")),"a")
    #     file.write("There is no last day's risk factors data. \n")
    #     file.close()
    #     return
    rfReport = pd.merge(memberInfo,tdRF,how='left')
    rfReport = pd.merge(rfReport,ytdRF,how='left')
    rfReport.sort_values(by=['CompanyCode','RFID'])

    if perRF['DataDate'].max()<tdDate:
        perRF = pd.concat([perRF,tdRF])
        perRF = perRF.groupby(['CompanyCode','DataDate']).tail(1)
        perRF = perRF.loc[perRF['DataDate']>tdDate-relativedelta(days=31),:]
    # else:
    #     file = open("QA_Log_{}.txt".format(tdDate.strftime("%Y%m%d")),"a")
    #     file.write("There is already as-of-date risk factors data in 30days_RF. \n")
    #     file.close()
 
    return rfReport, perRF