import os
import configparser
from flask import Flask, request, jsonify, json, Response
from flask_cors import CORS
from hana_ml import dataframe
from langchain.prompts import PromptTemplate
from langchain_core.output_parsers import StrOutputParser
from sql_formatter.core import format_sql
from gen_ai_hub.proxy.langchain.openai import ChatOpenAI
from gen_ai_hub.proxy.core.proxy_clients import get_proxy_client

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

# Establish a connection to SAP HANA
connection = dataframe.ConnectionContext(hanaURL, hanaPort, hanaUser, hanaPW)

# Initialize the proxy client and LLM model globally
proxy_client = get_proxy_client('gen-ai-hub')
llm = ChatOpenAI(proxy_model_name='gpt-4', temperature=0, proxy_client=proxy_client)

app = Flask(__name__)
CORS(app)

# Define the global variables
global_config = {}
global_ontology = None
global_properties = None
global_classes = None

def initialize_data():
    global global_config, global_ontology, global_properties, global_classes
    try:
        # Retrieve the configuration from the database
        cursor = connection.connection.cursor()
        cursor.execute("""
            SELECT ONTOLOGY_QUERY, PROPERTY_QUERY, CLASSES_QUERY, INSTRUCTIONS, PREFIXES, GRAPH, GRAPH_INFERRED, QUERY_EXAMPLE, TEMPLATE, QUERY_TEMPLATE, QUERY_TEMPLATE_NO_TOPIC, TEMPLATE_SIMILARITY 
            FROM ONTOLOGY_CONFIG
        """)
        config = cursor.fetchone()

        # Update the global configuration
        global_config = {
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
        }

        # Load ontology, properties, and classes
        cursor = connection.connection.cursor()
        result = cursor.callproc('SPARQL_EXECUTE', (global_config['ontology_query'], 'application/sparql-results+csv', '?', '?'))
        global_ontology = result[2]

        cursor = connection.connection.cursor()
        result = cursor.callproc('SPARQL_EXECUTE', (global_config['property_query'], 'application/sparql-results+json', '?', '?'))
        global_properties = result[2]

        cursor = connection.connection.cursor()
        result = cursor.callproc('SPARQL_EXECUTE', (global_config['classes_query'], 'application/sparql-results+json', '?', '?'))
        global_classes = result[0]

        print("Configuration and data loaded successfully.")
    except Exception as e:
        print(f"Error during initialization: {e}")
            
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
        
        # Use the global configuration
        ontology = global_ontology
        properties = global_properties
        classes = global_classes
        instructions = global_config.get('instructions')
        prefixes = global_config.get('prefixes')
        graph = global_config.get('graph')
        graph_inferred = global_config.get('graph_inferred')
        query_example = global_config.get('query_example')
        template_config = global_config.get('template')

        # Define the prompt template
        prompt_template = PromptTemplate(
            input_variables=["nl_query", "classes", "properties", "ontology", "graph", "graph_inferred", "prefixes", "query_example", "instructions"],
            template=template_config
        )

        # Create the LLM chain
        chain = prompt_template | llm
        
        # Run the chain with the provided inputs
        response = chain.invoke({"nl_query": nl_query, 
                                 "classes":classes, 
                                 "properties": properties, 
                                 "ontology": ontology, 
                                 "graph":graph, 
                                 "graph_inferred":graph_inferred, 
                                 "prefixes":prefixes, 
                                 "query_example":query_example, 
                                 "instructions":instructions})
        
        print("response.content: ", response.content)
        sparql_query = response.content.strip()
        print("sparql_query: ", sparql_query)

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
        
        # Use the global configuration
        ontology = global_ontology
        properties = global_properties
        classes = global_classes
        instructions = global_config.get('instructions')
        prefixes = global_config.get('prefixes')
        graph = global_config.get('graph')
        graph_inferred = global_config.get('graph_inferred')
        query_example = global_config.get('query_example')
        template = global_config.get('template')
        template_similarity = global_config.get('template_similarity')
        query_template = global_config.get('query_template')
        query_template_no_topic = global_config.get('query_template_no_topic')

        # Define the prompt template for topic extraction
        prompt_template_topic = PromptTemplate(
            input_variables=["question"],
            template=template_similarity
        )

        # Create the LLM chain for topic extraction
        chain_topic = prompt_template_topic | llm | StrOutputParser()

        # Run the chain with the provided inputs
        response_topic = chain_topic.invoke({'question': nl_query})
        
        # Strip out the formatting and extract the JSON content
        response_topic = response_topic.strip('```python\n').strip('\n```')
        
        # Parse the JSON response
        response_topic = json.loads(response_topic)
        
        # Extract the topic and query from the response
        topic = response_topic["topic"]
        query = response_topic["query"]

        # Define the prompt template for SPARQL query generation
        prompt_template_sparql = PromptTemplate(
            input_variables=["nl_query", "classes", "properties", "ontology", "graph", "graph_inferred", "prefixes", "query_example", "instructions"],
            template=template
        )
        
        # Create the LLM chain for SPARQL query generation
        chain_sparql = prompt_template_sparql | llm

        # Run the chain with the provided inputs
        response_sparql = chain_sparql.invoke({
            "nl_query": query,
            "classes": classes,
            "properties": properties,
            "ontology": ontology,
            "graph": graph,
            "graph_inferred": graph_inferred,
            "prefixes": prefixes,
            "query_example": query_example,
            "instructions": instructions
        })

        sparql_query = response_sparql.content.strip()
        
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

        return jsonify({'result': json.loads(result_json), 'final_query': final_query}), 200
    
    except Exception as e:
        return jsonify({'error': str(e), 'final_query': final_query}), 400

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
        query_template = data.get('query_template')
        query_template_no_topic = data.get('query_template_no_topic')
        template_similarity = data.get('template_similarity')

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
        cursor.execute(update_query, (ontology_query, property_query, classes_query, instructions, prefixes, graph, graph_inferred, query_example, template, query_template, query_template_no_topic, template_similarity))
        connection.connection.commit()
        return jsonify({'message': 'Configuration updated successfully'}), 200

    # Retrieve the current configuration values
    cursor.execute("SELECT ONTOLOGY_QUERY, PROPERTY_QUERY, CLASSES_QUERY, INSTRUCTIONS, PREFIXES, GRAPH, GRAPH_INFERRED, QUERY_EXAMPLE, TEMPLATE, QUERY_TEMPLATE, QUERY_TEMPLATE_NO_TOPIC, TEMPLATE_SIMILARITY FROM ONTOLOGY_CONFIG")
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
    
@app.route('/load_config', methods=['POST'])
def load_config():
    try:
        initialize_data()  # Call the initialization function
        return jsonify({'message': 'Configuration and data loaded successfully'}), 200
    except Exception as e:
        return jsonify({'error': str(e)}), 400
        
@app.route('/', methods=['GET'])
def root():
    return 'Embeddings API: Health Check Successfull.', 200

def create_app():
    return app

# Start the Flask app
if __name__ == '__main__':
    initialize_data()
    app.run('0.0.0.0', 8080)