import os
import configparser
from flask import Flask, request, jsonify, json, Response
from flask_cors import CORS
from hana_ml import dataframe
from SPARQLWrapper import SPARQLWrapper, JSON
from langchain.chains import LLMChain
from langchain.prompts import PromptTemplate
from gen_ai_hub.proxy.langchain.init_models import init_llm

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
        response_format = request.args.get('format', 'json')
        
        if not query:
            return jsonify({'error': 'Query is required'}), 400

        cursor = connection.connection.cursor()
        if response_format == 'csv':
            result = cursor.callproc('SPARQL_EXECUTE', (query, 'application/sparql-results+csv', '?', '?'))
            result_csv = result[2]
            return Response(result_csv, mimetype='text/csv')
        else:
            cursor = connection.connection.cursor()
            result = cursor.callproc('SPARQL_EXECUTE', (query, 'application/sparql-results+json', '?', '?'))
            result_json = result[2]
        
        return jsonify(json.loads(result_json)), 200
    
    except Exception as e:
        return jsonify({'error': str(e)}), 400

@app.route('/execute_sparql_query', methods=['GET'])
def execute_sparql_query():
    try:
        # Get the raw SQL query and format from the URL arguments
        query = request.args.get('query')
        response_format = request.args.get('format', 'json')

        if not query:
            return jsonify({'error': 'Query is required'}), 400

        cursor = connection.connection.cursor()
        if response_format == 'csv':
            result = cursor.callproc('SPARQL_EXECUTE', (query, 'application/sparql-results+csv', '?', '?'))
            result_csv = result[2]
            return Response(result_csv, mimetype='text/csv')
        else:
            result = cursor.callproc('SPARQL_EXECUTE', (query, 'application/sparql-results+json', '?', '?'))
            result_json = result[2]
            return jsonify(json.loads(result_json)), 200
    
    except Exception as e:
        return jsonify({'error': str(e)}), 400

@app.route('/translate_nl_to_sparql', methods=['POST'])
def translate_nl_to_sparql():
    try:
        # Get the natural language query and ontology from the request body
        data = request.get_json()
        nl_query = data.get('nl_query')
        ontology = data.get('ontology')

        if not nl_query or not ontology:
            return jsonify({'error': 'Natural language query and ontology are required'}), 400

        # Initialize the LLM model from SAP AI Hub
        llm = init_llm(model_name="gpt-4o")

        # Define the prompt template
        prompt_template = PromptTemplate(
            input_variables=["nl_query", "ontology"],
            template="""
            You are an expert in SPARQL queries. Given the following ontology and a user's question in natural language, generate a valid SPARQL query.

            Ontology:
            - Class: SAPPartner (has properties: accountName, partnerStatus, ofCountry)
            - Class: Country (has properties: countryName, belongsToSAPRegion)
            - Class: SAPRegion (has properties: regionName)
            - Relationship: ofCountry (SAPPartner → Country)
            - Relationship: belongsToSAPRegion (Country → SAPRegion)
            - Property: accountName (SAPPartner → String)
            - Property: partnerStatus (SAPPartner → String)
            - Property: countryName (Country → String)
            - Property: regionName (SAPRegion → String)

            User's Question: {nl_query}

            SPARQL Query:
            """
        )

        # Create the LLM chain
        chain = LLMChain(llm=llm, prompt=prompt_template)

        # Run the chain with the provided inputs
        response = chain.run({"nl_query": nl_query, "ontology": ontology})

        sparql_query = response.strip()

        # if not sparql_query.startswith("SELECT") and not sparql_query.startswith("CONSTRUCT") and not sparql_query.startswith("ASK") and not sparql_query.startswith("DESCRIBE"):
        #     return jsonify({'error': 'Failed to retrieve SPARQL query from LLM response', 'details': sparql_query}), 400

        return jsonify({'sparql_query': sparql_query}), 200
    
    except Exception as e:
        return jsonify({'error': str(e)}), 400

# @app.route('/chat_sparql_graph', methods=['GET'])
# def chat_sparql_graph():
#     try:
        
#     except Exception as e:
#         return jsonify({'error': str(e)}), 400
    
# @app.route('/consume_sparql_query', methods=['GET'])
# def consume_sparql_query():
#     try:
#         # Get the raw SQL query from the URL arguments
#         query = request.args.get('query')

#         if not query:
#             return jsonify({'error': 'Query is required'}), 400

#         # Use SPARQLWrapper to send the query to the execute_sparql_query endpoint
#         sparql = SPARQLWrapper("http://localhost:8080/execute_sparql_query")
#         sparql.setQuery(query)
#         sparql.setReturnFormat(JSON)
#         results = sparql.query().convert()

#         return jsonify(results), 200
    
#     except Exception as e:
#         return jsonify({'error': str(e)}), 400

@app.route('/', methods=['GET'])
def root():
    return 'Embeddings API: Health Check Successfull.', 200

def create_app():
    return app

# Start the Flask app
if __name__ == '__main__':
    app.run('0.0.0.0', 8080)