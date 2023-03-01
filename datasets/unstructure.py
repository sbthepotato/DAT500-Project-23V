import csv
from csv import DictReader

if __name__=="__main__":
	print('stuff')

	filename = '2022-01'

	with open('original/'+filename+'.csv' ,'r') as data:
		dict_reader = DictReader(data)
		list_of_dict = list(dict_reader)

	with open('scrambled/'+filename+'.txt', 'w') as f:
		for i in list_of_dict:
			for k, v in i.items(): 
				print(k, v, file=f)