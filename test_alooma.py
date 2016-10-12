#!/usr/bin/python

import sys, getopt
import alooma
import json
import string
import random


SF_TABLES_PATH = './views/tables.json'

SALESFORCE_TRANSFORM = './transform.py'

api_client = alooma.Alooma(hostname="#######", username="#######", password="#######")

MIXPANEL_COLUMNS = [
	{'columnName':'distinct_id'
		, 'distKey': True, 'primaryKey': False, 'sortKeyIndex': -1
		, 'columnType':{'type':'VARCHAR', 'length':256, 'nonNull': True}
	}
	,{'columnName':'event', 
		'distKey': False, 'primaryKey': False, 'sortKeyIndex': -1
		, 'columnType':{'type':'VARCHAR', 'length':256, 'nonNull': False}
	}
	,{'columnName':'initial_referring_domain', 
		'distKey': False, 'primaryKey': False, 'sortKeyIndex': -1
		, 'columnType':{'type':'VARCHAR', 'length':256, 'nonNull': False}
	}
	,{'columnName':'city', 
		'distKey': False, 'primaryKey': False, 'sortKeyIndex': -1
		, 'columnType':{'type':'VARCHAR', 'length':256, 'nonNull': False}
	}	
	,{'columnName':'region', 
		'distKey': False, 'primaryKey': False, 'sortKeyIndex': -1
		, 'columnType':{'type':'VARCHAR', 'length':256, 'nonNull': False}
	}	
	,{'columnName':'referrer', 
		'distKey': False, 'primaryKey': False, 'sortKeyIndex': -1
		, 'columnType':{'type':'VARCHAR', 'length':256, 'nonNull': False}
	}
	,{'columnName':'current_url', 
		'distKey': False, 'primaryKey': False, 'sortKeyIndex': -1
		, 'columnType':{'type':'VARCHAR', 'length':256, 'nonNull': False}
	}
	,{'columnName':'time', 
		'distKey': False, 'primaryKey': False, 'sortKeyIndex': 0
		, 'columnType':{'type':'FLOAT', 'nonNull': True}
	}	
	,{'columnName':'id', 
		'distKey': False, 'primaryKey': False, 'sortKeyIndex': -1
		, 'columnType':{'type':'VARCHAR', 'length':256, 'nonNull': True}
	}				
];


def name_generator(size=6, chars=string.ascii_lowercase + string.digits):
	return ''.join(random.choice(chars) for _ in range(size))

def addReshift(argv):
	hostname = ''
	port = '5439'
	schema_name = 'public'
	database_name = ''
	username = ''
	password = ''
	try:
		opts, args = getopt.getopt(argv,'d',[
			"hostname="
			,"port="
			,"schema_name="
			,"database_name="
			,"username="
			,"password="]
		)
	except getopt.GetoptError:
		print('Syntax: test_alooma.py -d --hostname some_host --port 5439 --schema_name public --database_name db --username user --password pass')
		sys.exit(2)

	print('adding Redshift')
	print(opts)
	for opt, arg in opts:
		name = name_generator(10)
		fields = {}
		
		if opt == '--hostname':
			hostname = arg
		elif opt == '--port':
			port = arg
		elif opt == '--schema_name':
			schema_name = arg
		elif opt == '--database_name':
			database_name = arg
		elif opt == '--username':
			username = arg
		elif opt == '--password':
			password = arg

	return api_client.set_redshift_config(hostname, port, schema_name, database_name, username, password)

def createSalesforce(source_name,args):
	fields = {
		'username': args[0],
		'password': args[1],
		'token': args[2],
		'client_id': args[3],
		'client_secret': args[4]
	}
	# create input source
	post_data = {
		"source": None,
		"target": str(api_client.get_transform_node_id()),
		"name": source_name,
		"type": "SALESFORCE",
		"configuration": {
			"username": fields['username'],
			"password": fields['password'],
			"token": fields['token'],
			"client_id": fields['client_id'],
			"fromDate": '2012-09-28',	#CHANGE THIS FOR PRODUCTION
			"client_secret": fields['client_secret'],
			"custom_objects": 'Account Lead Contact Opportunity Task'
			# "salesforce_client_redirect_uri": 'https://ap1.salesforce.com/services/oauth2/token' 
		}
		# https://test.salesforce.com/services/oauth2/success
		# custom_objects: 
	}
	id = api_client.create_input(input_post_data=post_data)
	# create a new table in Redshift
	with open(SF_TABLES_PATH) as tables_json:
		tables = json.load(tables_json)
		for table_name, columns in tables.items():
			print('CREATING A TABLE: '+table_name)
			api_client.create_table(table_name,columns)
	
	api_client.set_mapping_mode(flexible=True)

	# add transformation code
	f = open(SALESFORCE_TRANSFORM, 'r')
	code = f.read()
	api_client.set_transform(code)

	# create a mapping between input source and a table      
	with open('views/salesforce.json') as data_file:
		mappings = json.load(data_file)

	for mapping in mappings:
		print('ADDING A MAPPING: ' + mapping['mapping']['tableName'])
		print(api_client.set_mapping(mapping, mapping['name']))
	return id

def createMixpanel(source_name,args):
	fields = {}
	fields['client_id'] = args[0]
	fields['secret_id'] = args[1]
	# create input source
	id = api_client.create_mixpanel_input(
		fields['client_id'], 
		fields['secret_id'],
		'2015-03-01', 
		source_name,
		api_client.get_transform_node_id()
	)
	# create a new table in Redshift
	table_name = 'mixpanel_looker_alooma'

	api_client.create_table(table_name,MIXPANEL_COLUMNS)
	api_client.set_mapping_mode(flexible=True)

	# add transformation code
	f = open(SALESFORCE_TRANSFORM, 'r')
	code = f.read()
	api_client.set_transform(code)
	
	# create a mapping between input source and a table      
	with open('views/mixpanel.json') as data_file:
		mapping = json.load(data_file)
	mapping['name'] = source_name
	mapping['mapping']['tableName'] = table_name
	print(api_client.set_mapping(mapping, source_name))
	return id


def main(argv):
	inputfile = ''
	outputfile = ''
	try:
		opts, args = getopt.getopt(argv,"dcms",["clean","mixpanel","salesforce","hostname=","port=","schema_name=","database_name=","username=","password="])
	except getopt.GetoptError:
		print('Syntax: test_alooma.py -c')
		sys.exit(2)
	for opt, arg in opts:
		name = name_generator(10)
		fields = {}
		
		if opt == '-d':
			print(addReshift(argv))
		elif opt in ('-c','--clean'):
			## clearing redshift
			print(api_client.set_redshift_config(None, None, None, None, None, None, True))

			api_client.set_transform_to_default()
			# api_client.clean_restream_queue()
			api_client.remove_all_inputs()
			api_client.delete_all_event_types()

			# print(json.dumps(config))
			return None
		elif opt in ("-m", "--mixpanel"):
			id = createMixpanel('mixpanel_'+name,args)
			print(id)
		elif opt in ("-s", "--salesforce"):
			id = createSalesforce('salesforce_'+name,args)
			print(id)

		#    inputfile = arg


if __name__ == "__main__":
	 main(sys.argv[1:])