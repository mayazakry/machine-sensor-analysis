"""Flask web application."""
from flask import Flask
app = Flask(__name__)
@app.route('/')
def home():
    return {'status': 'Machine Sensor Analysis API Running'}
if __name__ == '__main__':
    app.run(host='0.0.0.0', port=80)