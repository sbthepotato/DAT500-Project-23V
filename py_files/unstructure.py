import os
from csv import DictReader

if __name__=="__main__":

	file_list = os.listdir('csv/')

	for file in file_list:
		if '.csv' in file:
			file = file.split('.')

			with open('csv/'+file[0]+'.csv' ,'r') as data:
				dict_reader = DictReader(data)
				list_of_dict = list(dict_reader)

			with open('txt/'+file[0]+'.txt', 'w') as f:
				for i in list_of_dict:
					for k, v in i.items(): 
						print(k, v, file=f)

			print(file[0])