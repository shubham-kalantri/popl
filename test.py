import pandas as pd
import matplotlib.pyplot as plt
class dataset:
	def __init__(self, file):
		self.file = file
		if file.endswith('.csv'):	
			self.df = pd.read_csv(file)
		elif file.endswith('.xlsx'):
			self.df = pd.read_excel(file)
		elif file.endswith('.json'):
			self.df = pd.read_json(file)
		elif file.endswith('.pickle'):
			self.df = pd.read_pickle(file)
		elif file.endswith('.dta'):
			self.df = pd.read_stata(file)
		elif file.endswith('.sas'):
			self.df = pd.read_sas(file)
		elif file.endswith('.parquet'):
			self.df = pd.read_parquet(file)
		elif file.endswith('.orc'):
			self.df = pd.read_orc(file)
		elif file.endswith('.xml'):
			self.df = pd.read_xml(file)
		else:
			print('Invalid file type')

	def dropRow(self, columns = []) -> None:
		if len(columns) == 0:
			self.df = self.df.dropna()
		else:
			self.df = self.df.dropna(subset = columns)
		self.df = self.df.reset_index(drop = True)

	def dropColumns(self, columns = []) -> None:
		self.df = self.df.drop(columns, axis = 1)
	
	def stdDate(self, column = 'Date', format = 'ddmmyyyy') -> None:
		fmt = '%d/%m/%Y'
		if format.lower() in ['dd/mm/yyyy', 'dd-mm-yyyy', 'dd.mm.yyyy', 'ddmmyyyy', 'dd mm yyyy']:
			fmt = '%d/%m/%Y'
		elif format.lower() in ['dd/mm/yy', 'dd-mm-yy', 'dd.mm.yy', 'ddmmyy', 'dd mm yy']:
			fmt = '%d/%m/%y'
		elif format.lower() in ['mm/dd/yyyy', 'mm-dd-yyyy', 'mm.dd.yyyy', 'mmddyyyy', 'mm dd yyyy']:
			fmt = '%m/%d/%Y'
		elif format.lower() in ['mm/dd/yy', 'mm-dd-yy', 'mm.dd.yy', 'mmddyy', 'mm dd yy']:
			fmt = '%m/%d/%y'
		elif format.lower() in ['yyyy/mm/dd', 'yyyy-mm-dd', 'yyyy.mm.dd', 'yyyymmdd', 'yyyy mm dd']:
			fmt = '%Y/%m/%d'
		elif format.lower() in ['yy/mm/dd', 'yy-mm-dd', 'yy.mm.dd', 'yymmdd', 'yy mm dd']:
			fmt = '%y/%m/%d'
		elif format.lower() in ['yy/dd/mm', 'yy-dd-mm', 'yy.dd.mm', 'yyddmm', 'yy dd mm']:
			fmt = '%y/%d/%m'
		self.df[column] = pd.to_datetime(self.df[column], dayfirst = True)
		self.df[column] = self.df[column].dt.strftime(fmt)

	def savetofile(self, file, type = 'csv') -> None:
		if type == 'csv':
			self.df.to_csv(file + '.csv', index = False)
		elif type == 'xlsx':
			self.df.to_excel(file + '.xlsx', index = False)
		elif type == 'json':
			self.df.to_json(file + '.json', orient = 'records', index = True, date_format = 'iso', force_ascii = False)
	
	def dropDuplicates(self, columns = []) -> None:
		if len(columns) == 0:
			self.df = self.df.drop_duplicates()
		else:
			self.df = self.df.drop_duplicates(subset = columns, keep='first')
	
	def normalise(self, columns = []):
		if len(columns) == 0:
			self.df = (self.df - self.df.min()) / (self.df.max() - self.df.min())
		else:
			self.df[columns] = (self.df[columns] - self.df[columns].min()) / (self.df[columns].max() - self.df[columns].min())
	
	def group(self, columns=[]):
		if len(columns)==0:
			print("Provide non empty attribute arguement for groupby")
		else:
			gb= self.df.groupby(columns)
		return gb

	def cleanName(self, column = 'Name') -> None:
		self.df[column] = self.df[column].str.replace(' ', '')
		for x in range(len(self.df[column])):
			i = 0
			while i < len(self.df[column][x]):
				if self.df[column][x][i].isupper() and i != 0:
					self.df[column][x] = self.df[column][x][:i] + ' ' + self.df[column][x][i:]
					i += 1
				i += 1

def getgroup(gb_obj,str):
	if(isinstance(gb_obj,pd.core.groupby.generic.DataFrameGroupBy)):
		gg=gb_obj.get_group(str)
		return gg 
	else:
		print("object type must be groupby to use functionality")