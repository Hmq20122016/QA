# -*- coding: utf-8 -*-
"""
Created on Mon Mar  7 18:01:56 2022

@author: mengqi
"""
import pandas as pd

def transform_code(x):
	x = list(map(lambda t: str(int(x[t])+1)[-1] ,range(len(x))))
	x.reverse()
	x = ['1']+x
	return  ''.join(x)

report_path = r'C:\Users\24588\Documents\AIG_QA_Report_20220307'
size_path = r'C:\Users\24588\Documents\size_data\risklevel.csv'
size_data = pd.read_csv(size_path)
size_data['mappingCompanyCode'] = ''
size_data.mappingCompanyCode = size_data.CompanyCode.apply(lambda x: x[12:])

report_data = pd.read_csv(report_path+'\\PD_Change.csv')
report_data['mappingCompanyCode'] = ''
report_data.mappingCompanyCode = report_data.CompanyCode.apply(lambda x: transform_code(x[11:]))



pdirMapping = pd.read_excel(r'C:\Users\24588\Downloads\OneDrive_2022-02-14\Quality Control on iRAP Daily Update\PDiR Mapping.xlsx', sheet_name="S&P")
bins = [pdirMapping.LB.iloc[0] / 1e4] + (pdirMapping.UB / 1e4).tolist()
labels = pdirMapping.PDiR.tolist()
labels = list(range(0,pdirMapping.shape[0]))
report_data['PDiR'] = pd.cut(report_data['PD'], bins, labels=labels)
report_data['ytdPDiR'] = pd.cut(report_data['ytdPD'], bins, labels=labels)

report_data['PDiR_rank'] = report_data['PDiR'].cat.codes
report_data['ytdPDiR_rank'] = report_data['ytdPDiR'].cat.codes

report_data['PDir_change'] = report_data['PDiR_rank'] - report_data['ytdPDiR_rank']

report_data = pd.merge(report_data, size_data[['mappingCompanyCode','RFValue']], how = 'left', on = 'mappingCompanyCode')
report_data['Important_Flag'] = 0

# report_data.loc[(report_data['RFValue']>=0.3) , 'Important_Flag'] = 1
# print(report_data.Important_Flag.sum())

# report_data.loc[ (report_data['PDir_change'].abs()>=2), 'Important_Flag'] = 1

# print(report_data.Important_Flag.sum())
report_data.loc[(report_data['RFValue']>=0.3) & (report_data['PDir_change'].abs()>=2), 'Important_Flag'] = 1


report_data = report_data.sort_values(by=['Important_Flag', 'RFValue'], ascending=False)
report_data.drop(['PDiR_rank', 'PDiR','ytdPDiR', 'ytdPDiR_rank','RFValue'], axis=1)
report_data.to_csv(report_path+'\\PD_Change_withfilter.csv', index = False)


a = pd.read_csv(r'C:\Users\24588\Downloads\Mapping Update Research\dtd\15.csv')
a = a[a.comp_no==199080]





