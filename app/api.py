import os
import configparser
from flask import Flask, request, jsonify, json, Response
from flask_cors import CORS
from hana_ml import dataframe
from langchain.chains import LLMChain
from langchain.prompts import PromptTemplate
from gen_ai_hub.proxy.langchain.init_models import init_llm
from sql_formatter.core import format_sql
from hdbcli import dbapi

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
        
        if not nl_query:
            return jsonify({'error': 'Natural language query required'}), 400

        # Retrieve the configuration from the database
        cursor = connection.connection.cursor()
        cursor.execute("SELECT ONTOLOGY_QUERY, PROPERTY_QUERY, CLASSES_QUERY, INSTRUCTIONS, PREFIXES, GRAPH, GRAPH_INFERRED, QUERY_EXAMPLE, TEMPLATE FROM ONTOLOGY_CONFIG")
        config = cursor.fetchone()

        ontology_query = config[0]
        property_query = config[1]
        classes_query = config[2]
        instructions = config[3]
        prefixes = config[4]
        graph = config[5]
        graph_inferred = config[6]
        query_example = config[7]
        template = config[8]

        # GET ONTOLOGY - Directly call the logic of execute_sparql_query
        cursor = connection.connection.cursor()
        result = cursor.callproc('SPARQL_EXECUTE', (ontology_query, 'application/sparql-results+csv', '?', '?'))
        ontology = result[2]

        # GET PROPERTIES - Directly call the logic of execute_sparql_query
        cursor = connection.connection.cursor()
        result = cursor.callproc('SPARQL_EXECUTE', (property_query, 'application/sparql-results+json', '?', '?'))
        properties = result[2]
        
        # GET CLASSES - Directly call the logic of execute_sparql_query
        cursor = connection.connection.cursor()
        result = cursor.callproc('SPARQL_EXECUTE', (classes_query, 'application/sparql-results+json', '?', '?'))
        classes = result[0]
        
        # Initialize the LLM model from SAP AI Hub
        llm = init_llm(model_name="gpt-4o")

        # Define the prompt template
        prompt_template = PromptTemplate(
            input_variables=["nl_query", "classes", "properties", "ontology", "graph", "graph_inferred", "prefixes", "query_example", "instructions"],
            template=template
        )

        # Create the LLM chain
        chain = LLMChain(llm=llm, prompt=prompt_template)

        # Run the chain with the provided inputs
        response = chain.run({"nl_query": nl_query, "classes":classes, "properties": properties, "ontology": ontology, "graph":graph, "graph_inferred":graph_inferred, "prefixes":prefixes, "query_example":query_example, "instructions":instructions})

        sparql_query = response.strip()

        return jsonify({'sparql_query': sparql_query}), 200
    
    except Exception as e:
        return jsonify({'error': str(e)}), 400

@app.route('/translate_nl_to_new', methods=['POST'])
def translate_nl_to_new():
    try:    
        # Get the natural language query
        data = request.get_json()
        nl_query = data.get('nl_query')
        
        if not nl_query:
            return jsonify({'error': 'Natural language query required'}), 400

        # Retrieve the configuration from the database
        cursor = connection.connection.cursor()
        cursor.execute("SELECT ONTOLOGY_QUERY, PROPERTY_QUERY, CLASSES_QUERY, INSTRUCTIONS, PREFIXES, GRAPH, GRAPH_INFERRED, QUERY_EXAMPLE, TEMPLATE, TEMPLATE_SIMILARITY, QUERY_TEMPLATE, QUERY_TEMPLATE_NO_TOPIC FROM ONTOLOGY_CONFIG")
        config = cursor.fetchone()

        ontology_query = config[0]
        property_query = config[1]
        classes_query = config[2]
        instructions = config[3]
        prefixes = config[4]
        graph = config[5]
        graph_inferred = config[6]
        query_example = config[7]
        template = config[8]
        template_similarity = config[9]
        query_template = config[10]
        query_template_no_topic = config[11]

        # GET ONTOLOGY - Directly call the logic of execute_sparql_query
        cursor = connection.connection.cursor()
        result = cursor.callproc('SPARQL_EXECUTE', (ontology_query, 'application/sparql-results+csv', '?', '?'))
        ontology = result[2]

        # GET PROPERTIES - Directly call the logic of execute_sparql_query
        cursor = connection.connection.cursor()
        result = cursor.callproc('SPARQL_EXECUTE', (property_query, 'application/sparql-results+json', '?', '?'))
        properties = result[2]
        
        # GET CLASSES - Directly call the logic of execute_sparql_query
        cursor = connection.connection.cursor()
        result = cursor.callproc('SPARQL_EXECUTE', (classes_query, 'application/sparql-results+json', '?', '?'))
        classes = result[0]
        
        # Initialize the LLM model from SAP AI Hub
        llm = init_llm(model_name="gpt-4o")

        # Define the prompt template
        prompt_template = PromptTemplate(
            input_variables=["nl_query"],
            template=template_similarity
        )

        # Create the LLM chain
        chain = LLMChain(llm=llm, prompt=prompt_template)

        # Run the chain with the provided inputs
        response_topic = chain.run({"question": nl_query})
        
        # Strip out the formatting and extract the JSON content
        response_topic = response_topic.strip('```python\n').strip('\n```')
        
        # Parse the JSON response
        response_topic = json.loads(response_topic)
        
        # Extract the topic and query from the response
        topic = response_topic["topic"]
        query = response_topic["query"]

        # Define the prompt template for SPARQL query generation
        prompt_template = PromptTemplate(
            input_variables=["query", "classes", "properties", "ontology", "graph", "graph_inferred", "prefixes", "query_example", "instructions"],
            template=template
        )
        
        # Create the LLM chain
        chain = LLMChain(llm=llm, prompt=prompt_template)

        # Run the chain with the provided inputs
        response = chain.run({"nl_query": query, "classes":classes, "properties": properties, "ontology": ontology, "graph":graph, "graph_inferred":graph_inferred, "prefixes":prefixes, "query_example":query_example, "instructions":instructions, "template":template})

        sparql_query = response.strip()
        
        if topic != "None":
            final_query = format_sql(query_template.format(generated_sparql_query=sparql_query, topic=topic))
        else:
            final_query = query_template_no_topic.format(generated_sparql_query=sparql_query)

        print("final_query: ", final_query)
        
        cursor = connection.connection.cursor()
        cursor.execute(final_query)
        
        # Fetch the results if needed
        result = cursor.fetchall()
        print("result: ", result)

        # Convert the result to JSON if needed
        result_json = json.dumps(result)

        return jsonify(json.loads(result_json)), 200
    
    except Exception as e:
        return jsonify({'error': str(e)}), 400

@app.route('/config', methods=['GET', 'POST'])
def config():
    cursor = connection.connection.cursor()
    
    if request.method == 'POST':
        # Update the configuration values
        data = request.get_json()
        ontology_query = data.get('ontology_query')
        property_query = data.get('property_query')
        classes_query = data.get('classes_query')
        instructions = data.get('instructions')
        prefixes = data.get('prefixes')
        graph = data.get('graph')
        graph_inferred = data.get('graph_inferred')
        query_example = data.get('query_example')
        template = data.get('template')

        update_query = """
        UPDATE ontology_config SET 
            ontology_query = ?, 
            property_query = ?, 
            classes_query = ?, 
            instructions = ?, 
            prefixes = ?, 
            graph = ?, 
            graph_inferred = ?, 
            query_example = ?,
            template = ?,
            query_template = ?,
            query_template_no_topic = ?,
            template_similarity = ?
        """
        cursor.execute(update_query, (ontology_query, property_query, classes_query, instructions, prefixes, graph, graph_inferred, query_example, template))
        connection.connection.commit()
        return jsonify({'message': 'Configuration updated successfully'}), 200

    # Retrieve the current configuration values
    cursor.execute("SELECT ONTOLOGY_QUERY, PROPERTY_QUERY, CLASSES_QUERY, INSTRUCTIONS, PREFIXES, GRAPH, GRAPH_INFERRED, QUERY_EXAMPLE, TEMPLATE FROM ONTOLOGY_CONFIG")    
    config = cursor.fetchone()
    return jsonify({
        'ontology_query': config[0],
        'property_query': config[1],
        'classes_query': config[2],
        'instructions': config[3],
        'prefixes': config[4],
        'graph': config[5],
        'graph_inferred': config[6],
        'query_example': config[7],
        'template': config[8],
        'query_template': config[9],
        'query_template_no_topic': config[10],
        'template_similarity': config[11]
    }), 200
    
@app.route('/', methods=['GET'])
def root():
    return 'Embeddings API: Health Check Successfull.', 200

def create_app():
    return app

# Start the Flask app
if __name__ == '__main__':
    app.run('0.0.0.0', 8080)