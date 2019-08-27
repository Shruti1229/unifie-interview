dict_json = {
		
		"first_name": "Ashwin",
		"table": "person",
		"last_name": "Kumar",
		"age": 27,
		"works_at": {
			"table": "company",
			"name": "ABC",
			"founded": "10-07-2015",
			"sector": "technology",
			"started_in": {
				"table": "country",
				"name": "Germany"
			}
		},
		"country_of_residence": {
			"table": "country",
			"name": "India"
		}
	}

def insert_query(table_name, field_value, values):
	"""
	Input : name of table, field names and values which are to be  inserted
	Output: prints the query
	"""
	columns = ','.join(str(x) for x in field_value)	
	val = ','.join(str(x) for x in values)
	i = 1
	if(table_name != 'NA'):
		print(f"INSERT IGNORE INTO {table_name} ({columns}) VALUES ({val})" + "\n")		
		return "str1"


def recursive_items(dictionary):
	"""
	Input : takes nested dictioanry 
	Output: return keys and values 
	"""
	for key, value in dictionary.items():
		if type(value) is dict:
			yield from recursive_items(value)
		else:
			yield (key, value)

def convert_dict_query(json):
	"""
	Input : nested dictionary of tables.
			Tables consists of attributes and values.

	output: calls insert_query for printing output
	"""
	field_value = []
	values = []
	table_name = "NA"
	for k, v in recursive_items(json):
		t_n = table_name
		if k == "table":
			table_name = v
			insert_query(t_n,field_value, values)
			field_value = []
			values = []		
		else:
			field_value.append(k)	
			values.append(v)
	insert_query(t_n,field_value, values)
convert_dict_query(dict_json)

"""
Output :

INSERT IGNORE INTO person (first_name,last_name,age) VALUES (Ashwin,Kumar,27)

INSERT IGNORE INTO company (name,founded,sector) VALUES (ABC,10-07-2015,technology)

INSERT IGNORE INTO country (name) VALUES (Germany)
"""
