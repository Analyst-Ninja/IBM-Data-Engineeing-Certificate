import os
from flask import Flask

app = Flask(__name__)

@app.route('/')
def say_hello():
    name = os.environ['Name']
    return f'Hello {name}\n'



if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8080, debug=True)
