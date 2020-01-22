from flask import Flask , render_template
app = Flask(__name__)
@app.route('/hello/<user>')
def hello_name(user):
    return render_template('hello.html', name = user)


@app.route('/hello/<int:score>')
def hello_score(score):
    return render_template('helloif.html', marks = score)
@app.route('/resu')
def result():
    dict1 = 'deepak'
    render_template('result.html', result = dict1 )

if __name__ == '__main__':
    app.run(debug = True)