import json
import mysql.connector
from mysql.connector import Error

"""
Program to retrieve elements from tables using json queries

INPUT :

{
  "table": "student",
  "enrolled_in": {
    "table": "enrolled",
    "enrolled_course": {
      "table": "course",
      "course_name": "OS",
      "taught_by": {
        "table": "faculty",
        "faculty_name": "ram"
      }
    }
  }
}

OUTPUT : 
{
  'student_name': 'xyz',
  'major': 'CS',
  'age': 18,
  'enrolled_in': {
    'course_id': 1
  },
  'enrolled_course': {
    'course_name': 'OS',
    'faculty_id': 1,
    'class_room_no': 201
  },
  'taught_by': {
    'faculty_name': 'ram',
    'salary': 10000,
    'joining_date': datetime.date(2018,8,8)
  }
}
"""


def main():
    json_query = {
  "table": "student",
  "enrolled_in": {
    "table": "enrolled",
    "enrolled_course": {
      "table": "course",
      "course_name": "OS",
      "taught_by": {
        "table": "faculty",
        "faculty_name": "ram"
      }
    }
  }
}

    connection = get_connection()

    output_format, json_list = get_conditions_list(json_query)
    select_attributes = get_attributes_from_table(connection, json_list)

    sql_query = create_query(json_list, select_attributes)
    output_rows = execute_sql_query(connection, sql_query)
    print(output_rows)

    json_outputs = items_to_json(output_format, output_rows, select_attributes)
    print(json_outputs)


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
    output_format = []
    for k, v in recursive_items(output_format,json):
        t_n = table_name
        if k == "table":
            table_name = v
            t = make_list(tab, t_n, field_value, values)
            field_value = []
            values = []
        else:
            field_value.append(k)
            values.append(v)

    t = make_list(tab, t_n, field_value, values)
    print(output_format)
    return output_format, t


def create_query(condition_list, select_attributes):
    """
    Create three separate clauses
    One each for SELECT, JOIN and WHERE
    """
    selected_columns = []
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
                                for table_id, table, column in select_attributes]
    select_attributes_string = ",".join(select_attributes_string)
    main_clause = SELECT_F.format(names=select_attributes_string, tablename=main_table_name)

    # iterate over required columns and create
    # join and where clauses
    for table_name, key, col, val in condition_list:
        if(len(join_clauses) == 0):
            t1 = main_table_name
            t2 = table_name
            join_clause = JOIN_F.format(table_name = table_name,
                                    t1 = main_table_name,
                                    t2 = table_name,
                                    col = key)
        else:
            t1 = t2
            t2 = table_name
            join_clause = JOIN_F.format(table_name=table_name,
                                        t1 = t1,
                                        t2 = t2,
                                        col=key)

        join_clauses.append(join_clause)


        if(col):
            where_clause = WHERE_F.format(t=table_name, col=col, val=val)

            if (len(where_clauses) == 0):
                where_clause = "WHERE " + where_clause
                print(where_clause)
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
    if (table_name != 'NA'):
        table_id = get_primary_id(table_name)
        tab.append([table_name, table_id, columns, val])
    return tab

def recursive_items(output_format, dictionary):
    """
    Input : takes nested dictioanry
    Output: return keys and values
    """
    for key, value in dictionary.items():
        if type(value) is dict:
            output_format.append(key)
            yield from recursive_items(output_format, value)
        else:
            yield (key, value)


def get_attributes_from_table(connection, json_list):
    table_column_tuples = []
    for i in json_list:
        table = i[0]
        query = "SHOW columns FROM {}".format(table)
        rows = execute_sql_query(connection, query)

        # returns each column as ['name', meta info]
        # we only need name of each column
        columns = [column[0] for column in rows]

        # Popping the first column as it contains id
        # we don't want to return id
        table_id = columns.pop(0)
        for column in columns:
            table_column_tuples.append((table_id, table, column))

    return table_column_tuples


def get_connection():
    connection = mysql.connector.connect(host='localhost', database='courses', user='root', password='')
    return connection


def execute_sql_query(connection, sql_query):
    """
    :param connection: establish connection with database
    :param sql_query: desired query
    :return: records (tuples) from the query
    """
    cursor = connection.cursor()
    cursor.execute(sql_query)
    records = cursor.fetchall()
    return records


def get_primary_id(table_name):
    """
    :param table_name: name of table
    :return: primary key of the table
    """
    connection = get_connection()
    query = "SHOW KEYS FROM {} WHERE Key_name = 'PRIMARY'".format(table_name)
    rows = execute_sql_query(connection, query)
    # returns each column as ['table name', 'key_type', 'name of key' etc]
    # we only need 'name of key' which is the 5th column of each row
    columns = [column[4] for column in rows]
    # Popping the first column as a table can contain multiple primary keys e.g enrolled(student_id, course_id)
    table_id = columns.pop(0)
    return table_id


def test_get_sql_from_json():
    usecase1 = ""
    expected1 = ""
    response1 = get_sql_from_json(usecase1)
    if response1 != expected1:
        print("Failed on usecase", usecase1, "expected", expected1, "response", response1)


def items_to_json(output_format,rows, attributes):
    """
    output formatter for response
    """
    table_name_headers = {"student": None,
                          "enrolled": "enrolled_in",
                          "course" : "enrolled_course",
                          "faculty" : "taught_by"
                          }
    print(output_format)

    response = {}
    for row in rows:
        for elem in zip(attributes, row):
            table, col, value = elem[0][1], elem[0][2], elem[1]
            header = table_name_headers.get(table, None)

            if header:
                response[header] = response.get(header, {})
                response[header][col] = value
            else:
                response[col] = value
    return response


if __name__ == '__main__':
    import sys

    if len(sys.argv) > 1 and sys.argv[1] == 'test':
        run_tests()
    else:
        main()
