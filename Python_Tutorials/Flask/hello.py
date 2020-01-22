from flask import Flask
app = Flask(__name__)

@app.route('/')
def hello_world():
    return ''


@app.route('/hello')
def hello_world1():
    return 'Hello World -Deepak1'

app.add_url_rule('/','hello',hello_world1)

if __name__ == '__main__':
    app.run()
