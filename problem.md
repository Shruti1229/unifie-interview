Given a JSON like

	{
		"table": "person",
		"first_name": "Ashwin",
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

and SQL tables like

	person
	| id | first_name | last_name | age | works_at | country_of_residence |
	
	company
	| id | name | sector | founded | started_in
	
	country
	| id | name |

(I) Write a function that will convert the JSON to the appropriate set of
SQL queries to insert the data into the database.

	def insert(json):
		pass

(II) Write a function that will take a JSON and convert it to a SQL
query that will return the matching records in JSON format.

	def select(json):
		pass

For example, if i call `select` like thisn

	select({
		"table": "person",
		"age": 30,
		"works_at": {
			"table": "company",
			"sector": "food",
			"started_in": {
				"table": "country",
				"name": "Germany"
			}
		}
	})

it should return results a list of JSON documents like

	{
		"table": "person",
		"first_name": "Senthil",
		"last_name": "Kumar",
		"age": 30,
		"works_at": {
			"table": "company",
			"name": "XYZ",
			"founded": "13-07-2019",
			"sector": "food",
			"started_in": {
				"table": "country",
				"name": "Poland"
			}
		},
		"country_of_residence": {
			"table": "country",
			"name": "Thailand"
		}
	}

"""
CREATE TABLES IF NOT EXISTS PERSON(ID INT AUTOINCREMENT,FIRST_NAME VARCHAR(30) NOT NULL,LAST_NAME VARCHAR(30),AGE INT, WORKS_AT
"""

CREATE TABLE PERSON(PERSON_ID INT NOT NULL,
FIRST_NAME VARCHAR(30), 
LAST_NAME VARCHAR(30), 
AGE INT,
COMPANY_ID INT,
PRIMARY KEY (PERSON_ID), 
COUNTRY_ID INT, 
CONSTRAINT STARTED_IN FOREIGN KEY (COUNTRY_ID) REFERENCES COUNTRY_OF_RESIDENCE (COUNTRY_ID), 
CONSTRAINT WORKS_AT FOREIGN KEY (COMPANY_ID) REFERENCES COMPANY (COMPANY_ID));

CREATE TABLE COMPANY(COMPANY_ID INT NOT NULL,
COMPANY_NAME VARCHAR(30), 
SECTOR VARCHAR(30), 
FOUNDED DATE, 
PRIMARY KEY (COMPANY_ID),
COUNTRY_ID INT, 
CONSTRAINT STARTED_IN FOREIGN KEY (COUNTRY_ID) REFERENCES COUNTRY_OF_RESIDENCE (COUNTRY_ID));



ALTER TABLE person
DROP FOREIGN KEY works_at,
CHANGE COLUMN company_id work_at INT(11) DEFAULT NULL,
ADD CONSTRAINT fk_company_id FOREIGN KEY (work_at) REFERENCES company(company_id)
