import os
import configparser
from flask import Flask, request, jsonify, json
from flask_cors import CORS
from hana_ml import dataframe
from SPARQLWrapper import SPARQLWrapper, JSON

# Check if the application is running on Cloud Foundry
if 'VCAP_APPLICATION' in os.environ:
    # Running on Cloud Foundry, use environment variables
    hanaURL = os.getenv('DB_ADDRESS')
    hanaPort = os.getenv('DB_PORT')
    hanaUser = os.getenv('DB_USER')
    hanaPW = os.getenv('DB_PASSWORD')
else:    
    # Not running on Cloud Foundry, read from config.ini file
    config = configparser.ConfigParser()
    config.read('config.ini')
    hanaURL = config['database']['address']
    hanaPort = config['database']['port']
    hanaUser = config['database']['user']
    hanaPW = config['database']['password']

# Step 1: Establish a connection to SAP HANA
connection = dataframe.ConnectionContext(hanaURL, hanaPort, hanaUser, hanaPW)

app = Flask(__name__)
CORS(app)

@app.route('/execute_query_raw', methods=['POST'])
def execute_query_raw():
    try:
        # Get the raw SQL query from the request body
        query = request.data.decode('utf-8')

        if not query:
            return jsonify({'error': 'Query is required'}), 400

        cursor = connection.connection.cursor()
        result = cursor.callproc('SPARQL_EXECUTE', (query, 'application/sparql-results+json', '?', '?'))
        result_json = result[2]
        
        return jsonify(json.loads(result_json)), 200
    
    except Exception as e:
        return jsonify({'error': str(e)}), 400

@app.route('/execute_sparql_query', methods=['GET'])
def execute_sparql_query():
    try:
        # Get the raw SQL query from the URL arguments
        query = request.args.get('query')

        if not query:
            return jsonify({'error': 'Query is required'}), 400

        cursor = connection.connection.cursor()
        result = cursor.callproc('SPARQL_EXECUTE', (query, 'application/sparql-results+json', '?', '?'))
        result_json = result[2]
        
        return jsonify(json.loads(result_json)), 200
    
    except Exception as e:
        return jsonify({'error': str(e)}), 400
    
@app.route('/consume_sparql_query', methods=['GET'])
def consume_sparql_query():
    try:
        # Get the raw SQL query from the URL arguments
        query = request.args.get('query')

        if not query:
            return jsonify({'error': 'Query is required'}), 400

        # Use SPARQLWrapper to send the query to the execute_sparql_query endpoint
        sparql = SPARQLWrapper("http://localhost:8080/execute_sparql_query")
        sparql.setQuery(query)
        sparql.setReturnFormat(JSON)
        results = sparql.query().convert()

        return jsonify(results), 200
    
    except Exception as e:
        return jsonify({'error': str(e)}), 400

@app.route('/', methods=['GET'])
def root():
    return 'Embeddings API: Health Check Successfull.', 200

def create_app():
    return app

# Start the Flask app
if __name__ == '__main__':
    app.run('0.0.0.0', 8080)