import json
import mysql.connector
from mysql.connector import Error


"""
Program to retrieve elements from tables using json queries

INPUT :
 
{
  "table": "person",
  "age": 30,
  "works_at": {
    "table": "company",
    "sector": "food",
    "started_in": {
      "table": "country",
      "name": "india"
    }
  }
}

OUTPUT : 
{
  'first_name': 'senthil',
  'last_name': 'kumar',
  'age': 30,
  'company_id': 1,
  'country_id': 5,
  'works_at': {
    'company_name': 'XYZ',
    'sector': 'food',
    'founded': 'datetime.date(2016, 7, 10)',
    'country_id': 5
  },
  'country_of_residence': {
    'name': 'india'
  }
}
"""

def main():
	json_query = {
		"table": "person",
		"age": 30,
		"works_at": {
			"table": "company",
			"sector": "food",
			"started_in": {
				"table": "country",
				"name": "india"
                          }
                    }
                 }
    
	json_list = get_conditions_list(json_query)
	print(json_list)
	connection = get_connection()
	select_attributes = get_attributes_from_table(connection, json_list)

	sql_query = create_query(json_list, select_attributes)
	print(sql_query)
	output_rows = execute_sql_query(connection, sql_query)
	print(output_rows)
	
	json_outputs = items_to_json(output_rows, select_attributes)
	print (json_outputs)	
	

def get_conditions_list(json):
	"""
	Input : nested dictionary of tables.
			Tables consists of attributes and values.

	output: returns list of required conditions
    	"""
	tab = []
	field_value = []
	values = []
	table_name = "NA"
	for k, v in recursive_items(json):
		t_n = table_name
		if k == "table":
			table_name = v
			t = make_list(tab,t_n,field_value, values)
			field_value = []
			values = []		
		else:
			field_value.append(k)	
			values.append(v)

	t = make_list(tab, t_n, field_value, values)
	print(t)
	return t

				
def create_query(condition_list, select_attributes):
	"""
	Create three separate clauses
	One each for SELECT, JOIN and WHERE
	"""
	
	join_clauses = []
	where_clauses = []
	
	# Formats for various clauses
	WHERE_F = "{t}.{col}=\"{val}\""
	JOIN_F = "JOIN {table_name} ON {t1}.{col}={t2}.{col}"
	SELECT_F = "SELECT {names} FROM {tablename}"
	QUERY_F = "{SELECT} \n {JOIN} \n {WHERE}"
	main_table = condition_list.pop(0)
	main_table_name = main_table[0]
	
	# Create a string of all field names to retrieve
	select_attributes_string = ["{table}.{column}".format(table=table, column=column) 
							for table, column in select_attributes]
	select_attributes_string = ",".join(select_attributes_string)
		
	main_clause = SELECT_F.format(names=select_attributes_string, tablename=main_table_name)
	
	# iterate over required columns and create 
	# join and where clauses
	for table_name, key, col, val  in condition_list:		
		join_clause = JOIN_F.format(table_name=table_name,
							t1=main_table_name, 
							t2=table_name, 
							col=key)
		
		join_clauses.append(join_clause)		
		
		where_clause = WHERE_F.format(t=table_name, col=col, val=val)
		
		if(len(where_clauses) == 0):
			where_clause = "WHERE " + where_clause
		else:
			where_clause = "AND " + where_clause
		where_clauses.append(where_clause)
	
	
	join_clauses = " \n".join(join_clauses)

	where_clauses = " \n".join(where_clauses)
	query = QUERY_F.format(SELECT=main_clause, JOIN=join_clauses, WHERE=where_clauses)
	return query


def make_list(tab, table_name, field_value, values):
	"""
	Input : name of table, field names and values which are to be  inserted
	Output: returns a list with table name, columns and values.
	"""
	columns = ','.join(str(x) for x in field_value)	
	val = ','.join(str(x) for x in values)
	if(table_name != 'NA'):
	    table_id  = get_primary_id(table_name)
	    tab.append([table_name,table_id,columns,val])
	return tab
	
	
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

	
def get_attributes_from_table(connection, json_list):
	table_column_tuples = []
	for i in json_list:
		table = i[0]
		connection = get_connection()
		query = "SHOW columns FROM {}".format(table)
		rows = execute_sql_query(connection, query)
		
		# returns each column as ['name', meta info]
		# we only need name of each column
		columns = [column[0] for column in rows]
		
		# Popping the first column as it contains id
		# we don't want to return id
		columns.pop(0)	
		for column in columns:
			table_column_tuples.append((table, column))
	
	return table_column_tuples


def get_connection():
    connection = mysql.connector.connect(host='localhost',database='json',user='root',password='') 
    return connection

def execute_sql_query(connection, sql_query):
	cursor = connection.cursor()
	cursor.execute(sql_query)
	records = cursor.fetchall()
	return records


def get_primary_id(table_name):
    # create table primary ids like table names
    table_id = table_name + '_id'
    return table_id

def test_get_sql_from_json():
	usecase1 = ""
	expected1 = ""
	response1 = get_sql_from_json(usecase1)
	if response1 != expected1:
		print ("Failed on usecase", usecase1, "expected", expected1, "response", response1)


def items_to_json(rows, attributes):
	"""
	hardcoded formatter 
	for response
	"""
	table_name_headers = { "person" : None, 
				"company" : "works_at", 
				"country" : "country_of_residence"
			     }
	response = {}
	for row in rows:
		for elem in zip(attributes, row):
			table, col, value = elem[0][0], elem[0][1], elem[1]
			header = table_name_headers.get(table, None)
			print(header)
			if header:
				response[header] = response.get(header, {})
				response[header][col] = value
			else:
				response[col] = value
	print(response)
	return response
			

if __name__ == '__main__':
	import sys
	if len(sys.argv) > 1 and sys.argv[1] == 'test':
		run_tests()
	else:
		main()
