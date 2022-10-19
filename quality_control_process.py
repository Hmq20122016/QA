# -*- coding: utf-8 -*-
"""
Created on Tue Feb 16 17:53:42 2021

@author: Kou Dena
"""
import os
import shutil

import pandas as pd
import datetime
from dateutil.relativedelta import relativedelta
import DataAnalytics_calculation.QA_tools.quality_control_tools_coverage as qc_coverage
import DataAnalytics_calculation.QA_tools.quality_control_tools_value as qc_value
import DataAnalytics_calculation.QA_tools.quality_control_tools_other as qc_other
import smtplib
import zipfile
from email import encoders
from email.mime.text import MIMEText
from email.mime.application import MIMEApplication

from email.message import Message
from email.mime.base import MIMEBase
from email.mime.multipart import MIMEMultipart    

def send_file_zipped(yyyymmdd):

	_user = 'criatdev@gmail.com'
	_pwd = 'criatdev@2019'
	_to = ['dnkou@criat.sg','rma@criat.sg']
	
	msg = MIMEMultipart()
	msg['Subject'] = 'IRAP_QA_{}'.format(yyyymmdd)
	msg['From'] = _user
	msg['To'] = ','.join(_to)
	part = MIMEText('IRAP QA report {}'.format(yyyymmdd))
	msg.attach(part)
	
	with open('QualityAssurance/Report/QA_{}.zip'.format(yyyymmdd),'rb') as file:
	# Attach the file with filename to the email
	    msg.attach(MIMEApplication(file.read(), Name='QA_Report.zip'))
	
	s = smtplib.SMTP('smtp.gmail.com:587')
	s.ehlo()
	s.starttls()
	s.login(_user,_pwd)
	s.sendmail(_user,_to,msg.as_string())
	s.close()

####### For test #######
# tdDate = datetime.datetime(2021,3,1) 

def archive_input(inputPath,arcPath,timeStamp):
	# timeStamp: when the pre-process begins in format of "%Y%m%d_%H%M%S"
	fn = lambda x:x+"_"+timeStamp
	shutil.make_archive(fn(arcPath+"/"+'QA'),format="zip",root_dir=inputPath)
	# remove input files.
	shutil.rmtree(inputPath)
# 	remove_input_file(inputPath)
	
def remove_input_file(inputPath):
	inputPathReg = os.listdir(inputPath)
	for kw in inputPathReg:
		if os.path.isdir(inputPath+"/"+kw):
			ff = os.listdir(inputPath+"/"+kw)
			for ic in ff:
				os.remove("/".join([inputPath,kw,ic]))
		else:
			os.remove("/".join([inputPath,kw]))

# tdDate = pd.to_datetime('20210502')
# prePath = './QA_Input/{}'.format('20210502')
# reportPath = './QA_Report/{}'.format('20210502')

def quality_control_process(tdDate, prePath,reportPath):
    # =================================================================================================================================
    # Settings
    # =================================================================================================================================
    
    tdDataPath = prePath
    reportPath = reportPath
    qc_value.check_folder(reportPath)
      
    # =================================================================================================================================
    # Prepare data
    # =================================================================================================================================
    fsAll = pd.read_csv('{}/fs.csv'.format(tdDataPath))
    
    histEntityInfo = pd.read_csv('EntityInfoAll.csv')
    tdEntityInfo = pd.read_csv('{}/EntityInfo.csv'.format(tdDataPath),usecols= ['CompanyCode','INDUSTRY_SECTOR_NUMBER','Exchange','CompanyName'] ) # after self.ready = True, namely new mapping table from CRI has been loaded into database
    tdEntityInfo.rename(columns = {'INDUSTRY_SECTOR_NUMBER':'INDUSTRY','Exchange':'REGION'},inplace = True) # after self.ready = True, namely new mapping table from CRI has been loaded into database
    exchangeName = pd.read_csv('CompanyInfo/Reference/ExchangeDomicile.csv',usecols=['CountryID','CountryName'])
    exchangeName['REGION'] = 'IRAP_SEC_REGION_'  + exchangeName['CountryID'].astype(str)
    exchangeName.rename(columns = {'CountryName':'REGION_Name'},inplace = True)
    # exchangeName.drop(columns = ['CountryID'],inplace = True)
    tdEntityInfo = pd.merge(tdEntityInfo,exchangeName, on  = 'REGION',how  ='left')
    industryName = pd.read_csv('CompanyInfo/Reference/BICS2007.csv',usecols=['SectorID','SectorName']).rename(columns = {'SectorID':'INDUSTRY','SectorName':'INDUSTRY_Name'})
    tdEntityInfo = pd.merge(tdEntityInfo,industryName, on  = 'INDUSTRY',how  ='left')
    tdEntityInfo.rename(columns = {'INDUSTRY_SECTOR_NUMBER':'INDUSTRY','Exchange':'REGION'},inplace = True) # after self.ready = True, namely new mapping table from CRI has been loaded into database
    tdEntityInfo = tdEntityInfo[['CompanyCode','INDUSTRY','REGION','CompanyName','REGION_Name','INDUSTRY_Name']]

    pdAll = pd.read_csv('{}/PD.csv'.format(tdDataPath))
    rfAll = pd.read_csv('rfAll.csv')
    activeDates = pd.read_csv('datesAll.csv')
    
    perAggPD = pd.read_csv('30days_aggPD.csv')
    aggPDAll = pd.read_csv('{}/AggPD.csv'.format(tdDataPath))
    #sectorMappingTable = pd.read_csv('{}/sectorMappingTable.csv'.format(dataPath))
    sectorMappingTableBICS2020 = pd.read_csv('sectorMappingTable_BICS2020.csv')
    aggList = pd.read_csv('CompanyInfo/Aggregate/aggList.csv')
    perPD = pd.read_csv('30days_PD.csv')
    mdfAll = pd.read_csv('{}/MDF.csv'.format(tdDataPath))
    
    
    # =================================================================================================================================
    # Quality control on financial statement dates
    # =================================================================================================================================   
    fsReport = qc_other.generate_fs_report(pdAll.copy(),tdEntityInfo,fsAll,tdDate)
    fsReport.to_csv('{}/FS_Report.csv'.format(reportPath),index=False)
    
    
    # =================================================================================================================================
    # Quality control on coverage
    # =================================================================================================================================
    
    # Compare new mapping table and historical one, 
    # update historical mapping table with new coming entities and information changed entities.
    histEntityInfo = qc_coverage.compare_mapping_table(histEntityInfo, tdEntityInfo, tdDate)
    histEntityInfo.to_csv('EntityInfoAll.csv',encoding='utf-8',index=False)
    
    # Compare consistency between risk factors and PD, 
    # return entities with risk factors but no PD
    rfAllPivot = rfAll.copy()
    rfAllPivot['CompanyID'] = rfAllPivot['CompanyCode'].apply(lambda x: int(x.split('_')[-1]))
    EtyWithRFnoPD = qc_coverage.get_entities_with_RF_no_PD(rfAllPivot, pdAll.copy(), tdEntityInfo, tdDate)
    EtyWithRFnoPD.to_csv('{}/Entities_With_Risk_Factors_No_PD.csv'.format(reportPath),index=False)
    
    # Compare consistency of total active PD over time, check the reason of inconsistency,
    # return 1.missing and new entites together with flag to indicate reasonable or not, 2.active number of yesterday and today at a global level and all industry
    MissingAndNewGlobal, activeNumberGlobal = qc_coverage.compare_coverage_daily_PD_global(activeDates.copy(), pdAll.copy(), histEntityInfo.copy(), EtyWithRFnoPD.copy(), tdDate)
    MissingAndNewGlobal.to_csv('{}/Global_Active_Number_Details.csv'.format(reportPath),index=False)
    
    # Compare consistency of active PD in each region and sector over time,
    # return 1.active number of yesterday and today in each region and sector, 
    #        2.suspicious region and sector need to be checked into individual firms (suspicious : a."ChangeInNumber" is the largest and filter out sector 
    #           b."ChangeInPct" is the largest and filter out sector c."ChangeInPct" is larger than a criteria 0.1 and number in the region-sector is larger than 100), 
    #        3.updated datesAll
    activeNumber, regionSectorToCheck, activeDates = qc_coverage.compare_coverage_daily_PD_sector(activeDates, pdAll.copy(), histEntityInfo.copy(), tdDate)
    activeNumber = pd.concat([activeNumberGlobal,activeNumber])
    activeNumber.to_csv('{}/Active_Number.csv'.format(reportPath),index=False)
    #activeDates.to_csv('{}/datesAll.csv'.format(dataPath),encoding='utf-8',index=False)
    
    # Check the reason of inconsistency in PD of each region and sector
    MissingAndNewAllSectors = qc_coverage.compare_daily_entities_with_PD(regionSectorToCheck, activeDates, histEntityInfo.copy(), EtyWithRFnoPD.copy(), tdDate)
    MissingAndNewAllSectors.to_csv('{}/Region_Industry_Active_Number_Details.csv'.format(reportPath),index=False)
    
    
    # =================================================================================================================================
    # Quality control on value based on BICS2020
    # =================================================================================================================================
    
    # Grasp suspicious jump in aggregate PD (mean type),
    # return 1.region and sector list with suspicious jump in aggregate PD
    #        2.updated 30-day aggregate PD
    aggPdReport, perAggPD = qc_value.get_agg_PD_change_BICS2020(perAggPD, aggPDAll, sectorMappingTableBICS2020, tdDate)
    aggPdReport.to_csv('{}/Agg_PD_Change.csv'.format(reportPath),index=False)
    perAggPD.to_csv('30days_aggPD.csv',encoding='utf-8',index=False)
    
    # Grasp all jumps in individual PD for those region and sector with suspicious jump in aggregate PD,
    # return entitly list with PD changes for those region and sector with suspicious jump in aggregate PD
    tdMappingTableBICS2020 = qc_other.get_company_BICS2020_information(aggList,sectorMappingTableBICS2020)
    pdInAggSusp, _ = qc_value.get_individual_PD_change_BICS2020(perPD.copy(), pdAll.copy(), histEntityInfo.copy(), tdMappingTableBICS2020.copy(), tdDate, 
                                                                regionSectorToCheck=aggPdReport[['REGION_Name','INDUSTRY_LEVEL_1_Name','INDUSTRY_LEVEL_2_Name']])
    pdInAggSusp.to_csv('{}/Agg_PD_Change_Details.csv'.format(reportPath),index=False)
    
    # Plot MDF and grasp risk factors for member entities of those region and sector with suspicious jump in aggregate PD
    rfAll['DataDate'] = pd.to_datetime(rfAll['DataDate'])
    rfAllUnpivot = rfAll[rfAll['DataDate']==tdDate].copy()
    rfAllUnpivot = rfAllUnpivot.melt(id_vars=['CompanyCode','DataDate'],var_name='RFID',value_name='RFValue')
    perRF = rfAll[rfAll['DataDate'] == tdDate-relativedelta(days=1)].copy()
    perRF = perRF.melt(id_vars=['CompanyCode','DataDate'],var_name='RFID',value_name='RFValue')
    
    regionSectorToCheck = aggPdReport[['REGION_Name','INDUSTRY_LEVEL_1_Name','INDUSTRY_LEVEL_2_Name']].reset_index(drop=True)
    for i in regionSectorToCheck.index:
    
        # Plot MDF figures for member entities of those region and sector with suspicious jump in aggregate PD
        savePath = ('{}/Agg_PD_Change_MDF&RF/{}_{}_{}'.format(reportPath,regionSectorToCheck.loc[i,'REGION_Name'],regionSectorToCheck.loc[i,'INDUSTRY_LEVEL_1_Name'],
                                                           regionSectorToCheck.loc[i,'INDUSTRY_LEVEL_2_Name']))
        qc_value.check_folder(savePath)
        memberInfo = pdInAggSusp.loc[(pdInAggSusp.REGION_Name==regionSectorToCheck.loc[i,'REGION_Name'])&
                                     (pdInAggSusp.INDUSTRY_LEVEL_1_Name==regionSectorToCheck.loc[i,'INDUSTRY_LEVEL_1_Name'])&
                                     (pdInAggSusp.INDUSTRY_LEVEL_2_Name==regionSectorToCheck.loc[i,'INDUSTRY_LEVEL_2_Name']),
                                     ['CompanyCode','CompanyName']]
        qc_value.plot_MDF_of_list(mdfAll.copy(), memberInfo, savePath)
        # Grasp yesterday and today's risk factors for member entities of those region and sector with suspicious jump in aggregate PD
        rfReport, _ = qc_value.get_risk_factors_of_list(perRF.copy(), rfAllUnpivot.copy(), memberInfo, tdDate)
        rfReport.to_csv('{}/Risk_Factors_Change.csv'.format(savePath),index=False)
    
    # Grasp suspicious jump in individual PD,
    # return 1.entity list with suspicious jump in PD
    #        2.updated 30-day PD 
    pdReport, perPD = qc_value.get_individual_PD_change_BICS2020(perPD, pdAll.copy(), histEntityInfo.copy(), tdMappingTableBICS2020.copy(), tdDate, regionSectorToCheck=pd.DataFrame([]))
    pdReport.to_csv('{}/PD_Change.csv'.format(reportPath),index=False)
    perPD.to_csv('30days_PD.csv',encoding='utf-8',index=False)
    
    # Plot MDF figures for individual entities with suspicious jump in PD
    savePath = ('{}/PD_Change_MDF&RF'.format(reportPath))
    qc_value.check_folder(savePath)
    memberInfo = pdReport[['CompanyCode','CompanyName']].copy()
    qc_value.plot_MDF_of_list(mdfAll, memberInfo, savePath)
    
    # Grasp yesterday and today's risk factors for individual entities with suspicious jump in PD, and update 30-day risk factors
    rfReport, _ = qc_value.get_risk_factors_of_list(perRF, rfAllUnpivot, memberInfo, tdDate)
    rfReport.to_csv('{}/Risk_Factors_Change.csv'.format(savePath),index=False)
    
        
    archive_input(inputPath = 'QualityAssurance/Report/{}'.format(tdDate.strftime('%Y%m%d')),arcPath ='QualityAssurance/Report' ,timeStamp = tdDate.strftime('%Y%m%d'))
    send_file_zipped(yyyymmdd=tdDate.strftime('%Y%m%d'))
    
    
        
        
    
    # # =================================================================================================================================
    # # Quality control on value based on BICS2007
    # # =================================================================================================================================
    
    # # Grasp suspicious jump in aggregate PD (mean type),
    # # return 1.region and sector list with suspicious jump in aggregate PD
    # #        2.updated 30-day aggregate PD
    # aggPdReport, perAggPD = qc_value.get_agg_PD_change(perAggPD, aggPDAll, sectorMappingTable, tdDate)
    # aggPdReport.to_csv('{}/Agg_PD_Change.csv'.format(reportPath),index=False)
    # perAggPD.to_csv('{}/30days_aggPD.csv'.format(dataPath),encoding='utf-8',index=False)
    
    # # Grasp all jumps in individual PD for those region and sector with suspicious jump in aggregate PD,
    # # return entitly list with PD changes for those region and sector with suspicious jump in aggregate PD
    # pdInAggSusp, _ = qc_value.get_individual_PD_change(perPD.copy(), pdAll.copy(), histEntityInfo.copy(), tdDate, regionSectorToCheck=aggPdReport[['REGION_Name','INDUSTRY_Name']])
    # pdInAggSusp.to_csv('{}/Agg_PD_Change_Details.csv'.format(reportPath),index=False)
    
    # # Plot MDF and grasp risk factors for member entities of those region and sector with suspicious jump in aggregate PD
    # rfAll['DataDate'] = pd.to_datetime(rfAll['DataDate'])
    # rfAllUnpivot = rfAll[rfAll['DataDate']==tdDate].copy()
    # rfAllUnpivot = rfAllUnpivot.melt(id_vars=['CompanyCode','DataDate'],var_name='RFID',value_name='RFValue')
    # perRF = rfAll[rfAll['DataDate'] == tdDate-relativedelta(days=1)].copy()
    # perRF = perRF.melt(id_vars=['CompanyCode','DataDate'],var_name='RFID',value_name='RFValue')
    
    # regionSectorToCheck = aggPdReport[['REGION_Name','INDUSTRY_Name']].reset_index(drop=True)
    # for i in regionSectorToCheck.index:
    #     # Plot MDF figures for member entities of those region and sector with suspicious jump in aggregate PD
    #     savePath = ('{}/Agg_PD_Change_MDF&RF/{}_{}'.format(reportPath,regionSectorToCheck.loc[i,'REGION_Name'],regionSectorToCheck.loc[i,'INDUSTRY_Name']))
    #     qc_value.check_folder(savePath)
    #     memberInfo = pdInAggSusp.loc[(pdInAggSusp.REGION_Name==regionSectorToCheck.loc[i,'REGION_Name'])&(pdInAggSusp.INDUSTRY_Name==regionSectorToCheck.loc[i,'INDUSTRY_Name']),
    #                                  ['CompanyCode','CompanyName']]
    #     qc_value.plot_MDF_of_list(mdfAll.copy(), memberInfo, savePath)
    #     # Grasp yesterday and today's risk factors for member entities of those region and sector with suspicious jump in aggregate PD
    #     rfReport, _ = qc_value.get_risk_factors_of_list(perRF.copy(), rfAllUnpivot.copy(), memberInfo, tdDate)
    #     rfReport.to_csv('{}/Risk_Factors_Change.csv'.format(savePath),index=False)
    
    # # Grasp suspicious jump in individual PD,
    # # return 1.entity list with suspicious jump in PD
    # #        2.updated 30-day PD 
    # pdReport, perPD = qc_value.get_individual_PD_change(perPD, pdAll.copy(), histEntityInfo.copy(), tdDate, regionSectorToCheck=pd.DataFrame([]))
    # pdReport.to_csv('{}/PD_Change.csv'.format(reportPath),index=False)
    # perPD.to_csv('{}/30days_PD.csv'.format(dataPath),encoding='utf-8',index=False)
    
    # # Plot MDF figures for individual entities with suspicious jump in PD
    # savePath = ('{}/PD_Change_MDF&RF'.format(reportPath))
    # qc_value.check_folder(savePath)
    # memberInfo = pdReport[['CompanyCode','CompanyName']].copy()
    # qc_value.plot_MDF_of_list(mdfAll, memberInfo, savePath)
    
    # # Grasp yesterday and today's risk factors for individual entities with suspicious jump in PD, and update 30-day risk factors
    # rfReport, _ = qc_value.get_risk_factors_of_list(perRF, rfAllUnpivot, memberInfo, tdDate)
    # rfReport.to_csv('{}/Risk_Factors_Change.csv'.format(savePath),index=False)
    
    # =================================================================================================================================
    # Quality control is done!
    # =================================================================================================================================

