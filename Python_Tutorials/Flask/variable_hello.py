from flask import Flask
app = Flask(__name__)
# if you provide the name after the root then it will print your name in the browser.
@app.route('/<name>')
def hello_world(name):
    return 'Hello %s !!!' % name
#If you provide url /blog/ <other than int> then it throws the error. in case of integer it will gives you the postid
@app.route('/blog/<int:postID>')
def show_blog(postID):
    return 'Blog Number %d' % postID
#If you provide url /rev/ <other than int> then it throws the error. in case of float it will gives you the postid
@app.route('/rev/<float:revNo>')
def revision(revNo):
    return 'Revision Number %f ' % revNo
#It will provide the result if you give you the exact url
@app.route('/flask')
def hello_flask():
    return 'Hello Flask!'
#In url if you provide http://127.0.0.1:5000/python or http://127.0.0.1:5000/python/ it will provide the result.
#In case of flask if you try http://127.0.0.1:5000/flask/ url then it throws the error. 

@app.route('/python/')
def hello_python():
    return 'hello python!'


if __name__ =='__main__':
    app.run(debug=True)