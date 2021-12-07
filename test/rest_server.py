from flask import Flask, request, flash, url_for, redirect, render_template
import json 


app = Flask(__name__)
app.config['SECRET_KEY'] = 'my secret'


@app.route('/test', methods = ['GET', 'POST'])
def new():
    if request.method == 'POST':
        print(json.dumps(request.json, indent=4, sort_keys=True))

        return {'POST': 'ok'}
    return {'Endpoint is running': 'ok'}


if __name__ == '__main__':
    app.run(debug = True)
    