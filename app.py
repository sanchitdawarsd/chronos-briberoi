import pandas as pd
from flask import Flask, jsonify
from application_logging.logger import logger
import yaml

# Params
params_path = "params.yaml"

def read_params(config_path):
    with open(config_path) as yaml_file:
        config = yaml.safe_load(yaml_file)
    return config

config = read_params(params_path)    

try:
    app = Flask(__name__)
    roi_data = config["files"]["roi_data"]

    dff = pd.read_csv(roi_data)
    @app.route('/api/data', methods=['GET'])
    def get_data():
    # Convert DataFrame to JSON and return it
      return jsonify(dff.to_dict(orient='records'))
    if __name__ == '__main__':
      app.run(debug=True, port=8080)  
    
    # Retrieve the port number
    port = app.url_map._rules[0].arguments['port']
    print(f"The app is running on port {port}")
except Exception as e:
    logger.error(
        "Error occurred during Pair Data Fusion process. Error: %s" % e, exc_info=True)
