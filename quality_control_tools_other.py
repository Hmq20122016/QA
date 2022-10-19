# -*- coding: utf-8 -*-
"""
Created on Tue Mar  2 15:00:42 2021

@author: admin
"""

import pandas as pd


def get_company_BICS2020_information(aggList,sectorMappingTableBICS2020):
    
    '''
    

    Parameters
    ----------
    aggList : DataFrame
        DESCRIPTION: today's mapping link between company and aggregation model
        with columns ['CompanyCode','ObjectID']
    sectorMappingTableBICS2020 : DataFrame
        DESCRIPTION: region and industry (BICS2020) description for each aggregation model
        with columns ['ObjectID', 'REGION_Name', 'INDUSTRY_LEVEL_1_Name','INDUSTRY_LEVEL_2_Name']

    Returns
    -------
    tdMappingTableBICS2020 : DataFrame
        DESCRIPTION: region and industry (BICS2020) information for each company
        with columns ['CompanyCode', 'REGION_Name', 'INDUSTRY_LEVEL_1_Name','INDUSTRY_LEVEL_2_Name']

    '''
    
    aggList['Len'] = aggList['ObjectID'].apply(lambda x:len(x))
    aggList.sort_values(by=['CompanyCode','Len'],inplace=True)
    tdMappingTableBICS2020 = aggList.drop_duplicates(subset='CompanyCode',keep='last')
    tdMappingTableBICS2020 = pd.merge(tdMappingTableBICS2020,sectorMappingTableBICS2020,how='left')
    tdMappingTableBICS2020.loc[tdMappingTableBICS2020['INDUSTRY_LEVEL_1_Name']=='All',['INDUSTRY_LEVEL_1_Name','INDUSTRY_LEVEL_2_Name']] = ['Others','Others']
    tdMappingTableBICS2020.loc[tdMappingTableBICS2020['INDUSTRY_LEVEL_2_Name']=='All',['INDUSTRY_LEVEL_2_Name']] = 'Others'
    tdMappingTableBICS2020.drop(columns=['ObjectID','Len'],inplace=True)
    
    return tdMappingTableBICS2020

def generate_fs_report(pdAll,tdEntityInfo,fsAll,tdDate):
    
    '''

    Parameters
    ----------
    pdAll : DataFrame
        DESCRIPTION: today's individual PD
        with columns ['CompanyCode','DataDate','Horizon','ForwardPoint','PD']
    tdEntityInfo : DataFrame
        DESCRIPTION: today's company information
        with columns ['CompanyCode','INDUSTRY','REGION','Database','CompanyName','REGION_Name','INDUSTRY_Name']
    fsAll : DataFrame
        DESCRIPTION: financial statement date
        with columns ['CompanyCode','Update_Date','FS_type']
    tdDate : datetime
        DESCRIPTION： as-of-date of PD

    Returns
    -------
    fsReport : DataFrame
        DESCRIPTION: report on update date of financial statements of individual companies
        with columns ['CompanyName','DataDate','REGION_Name','INDUSTRY_Name','FS_type','Update_Date','DeltaMonth']

    '''
 	
    pdAll['DataDate'] = pd.to_datetime(pdAll['DataDate'])
    pdAll = pdAll.loc[(pdAll['DataDate']==tdDate)&(pdAll['Horizon']=='12M')&(pdAll['ForwardPoint']==0),['CompanyCode','DataDate','PD']]
    # if pdAll.empty:
    #     file = open("QA_Log_{}.txt".format(tdDate.strftime("%Y%m%d")),"a")
    #     file.write("There is no as-of-date PD data. \n")
    #     file.close()
    #     return
    fsAll['Update_Date'] = pd.to_datetime(fsAll['Update_Date'])
    if ~isinstance(fsAll,type(None)):
        fsReport = pd.merge( fsAll,pdAll[['CompanyCode','DataDate']].drop_duplicates())
        fsReport = pd.merge(fsReport,tdEntityInfo)
        fsReport = fsReport.dropna()
        fsReport['FS_type'] = fsReport['FS_type'].apply(lambda x: str(x)[0])
        fsReport['Update_Month'] = fsReport['Update_Date'].apply(lambda x: x.year*12+x.month)
        fsReport['DataMonth'] = fsReport['DataDate'].apply(lambda x: x.year*12+x.month)
        fsReport['DeltaMonth'] = fsReport['DataMonth']-fsReport['Update_Month']
        fsReport['Problem'] = 0
        fsReport.loc[(fsReport['FS_type'] == 'Q')&(fsReport['DeltaMonth']>4),'Problem'] = 1
        fsReport.loc[(fsReport['FS_type'] == 'S')&(fsReport['DeltaMonth']>7),'Problem'] = 1
        fsReport.loc[(fsReport['FS_type'] == 'A')&(fsReport['DeltaMonth']>13),'Problem'] = 1
        fsReport = fsReport.loc[fsReport['Problem']==1,['CompanyName','DataDate','REGION_Name','INDUSTRY_Name','FS_type','Update_Date','DeltaMonth']]
    else:
        fsReport = pd.DataFrame()
    return fsReport
