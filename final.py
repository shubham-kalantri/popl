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

	def cleanName(self, column = 'Name', tokenize = False) -> None:
		self.df[column] = self.df[column].str.replace(' ', '')
		for x in range(len(self.df[column])):
			i = 0
			while i < len(self.df[column][x]):
				if self.df[column][x][i].isupper() and i != 0:
					self.df[column][x] = self.df[column][x][:i] + ' ' + self.df[column][x][i:]
					i += 1
				i += 1
		if tokenize:
			self.df[column] = self.df[column].str.split(' ')
		for i in range(len(self.df[column])):
			if len(self.df[column][i]) == 2:
				self.df[column][i].insert(1, '')
		self.df['First Name'] = self.df[column].str[0]
		self.df['Middle Name'] = self.df[column].str[1]
		self.df['Last Name'] = self.df[column].str[2]        
		self.df.drop(column, axis = 1, inplace = True)
	
	def graph(self, x, y = [], type = 'line', Legend = True, Labels = True, bins = 0) -> None:
		if type.lower() == 'line':
			_ = plt.plot(self.df[x], self.df[y])
			if Labels:
				_ = plt.xlabel(x)
				_ = plt.ylabel(y)
			_ = plt.xlim(self.df[x].min() - 1, self.df[x].max() + 1)
			_ = plt.ylim(self.df[y].min() - 1, self.df[y].max() + 1)
			if Legend:
				_ = plt.legend([x, y])
			_ = plt.show()
		elif type.lower() == 'scatter':
			_ = plt.scatter(self.df[x], self.df[y])
			if Labels:
				_ = plt.xlabel(x)
				_ = plt.ylabel(y)
			_ = plt.xlim(self.df[x].min() - 1, self.df[x].max() + 1)
			_ = plt.ylim(self.df[y].min() - 1, self.df[y].max() + 1)
			if Legend:
				_ = plt.legend([x, y])
			_ = plt.show()
		elif type.lower() == 'bar':
			_ = plt.bar(self.df[x], self.df[y])
			if Labels:
				_ = plt.xlabel(x)
				_ = plt.ylabel(y)
			_ = plt.xlim(self.df[x].min() - 1, self.df[x].max() + 1)
			_ = plt.ylim(self.df[y].min() - 1, self.df[y].max() + 1)
			if Legend:
				_ = plt.legend([x, y])
			_ = plt.show()
		elif type.lower() == 'histogram':
			_ = plt.hist(self.df[x], bins)
			if Labels:
				_ = plt.ylabel(x)
			_ = plt.xlim(self.df[x].min() - 1, self.df[x].max() + 1)
			if Legend:
				_ = plt.legend([x, y])
			_ = plt.show()
		elif type.lower() == 'pie':
			_ = plt.pie(self.df[x], labels = self.df[y])
			_ = plt.show()
			if Legend:
				_ = plt.legend([x, y])
		else:
			print('Invalid Type')

	def select(self, column, where, value, like = None):
		for i in range(len(self.df[column])):
			if self.df[where][i] == value:
				if like == None:
					print(self.df[column][i])
				else:
					for x in like:
						print(self.df[column][i])
						break

def getgroup(gb_obj,str):
	if(isinstance(gb_obj,pd.core.groupby.generic.DataFrameGroupBy)):
		gg=gb_obj.get_group(str)
		return gg 
	else:
		print("object type must be groupby to use functionality")

def command_handler(dset : dataset) -> None:
	# command handler
	while True:
		print("type 'list' to list all commands")
		print("Enter command: ")
		x = input()
		if x == 'list':
			# list all commands
			print("Commands: ")
			print("group: group by")
			print("clean: clean name")
			print("graph: graph")
			print("getgroup: get group")
			print("quit: quit")
		else:
			params = x.split(' ')
			if params[0] == 'group':
				# group by
				gb = dset.group(params[1::])
				print(gb)
			elif params[0] == 'clean':
				# clean name
				dset.clean_name(params[1], params[2])
			elif params[0] == 'graph':
				# graph
				dset.graph(params[1], params[2], params[3])
			elif params[0] == 'getgroup':
				# get group
				gg = getgroup(gb, params[1])
				print(gg)
			elif params[0] == 'quit':
				# quit
				print("Quitting command handler\n...")
				break
			else:
				print("Invalid command")

if __name__ == '__main__':
	while True:
		# print list of commands
		print("Commands: ")
		print("rf: read file")
		print("h: help")
		print("q: quit")
		x = input("Enter command: ")
		if x == 'rf':
			# read file
			file = input("Enter file name: ")
			# get file extension
			ext = file.split('.')[-1]
			if ext in ['csv', 'xlsx', 'json', 'pickle', 'dta', 'sas', 'parquet', 'orc', 'xml']:
				# create dataset object
				obj = dataset(file)
				# call command handler
				command_handler(obj)
			else:
				print("Invalid file type")
		elif x == 'h':
			# help
			print("help")
		elif x == 'q':
			# quit
			break
		else:
			print("Invalid command")
		