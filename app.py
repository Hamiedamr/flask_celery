from flask import Flask, request, render_template, session, redirect, url_for, redirect, jsonify, make_response
from flask_sqlalchemy import SQLAlchemy
from celery import Celery
from flask_caching import Cache
import os

app = Flask(__name__)
cache = Cache(app)
app.config['SECRET_KEY'] = os.environ['SECRET_KEY']
app.config['CACHE_TYPE'] = os.environ['CACHE_TYPE']
app.config['CACHE_DEFAULT_TIMEOUT'] = os.environ['CACHE_DEFAULT_TIMEOUT']

# Celery configuration
app.config['CELERY_BROKER_URL'] =os.environ['CELERY_BROKER_URL']
app.config['CELERY_RESULT_BACKEND'] = os.environ['CELERY_RESULT_BACKEND']
# Database connection
app.config['SQLALCHEMY_DATABASE_URI'] = os.environ['DATABASE_URL']
db = SQLAlchemy(app)

# Initialize Celery
celery = Celery(app.name, broker=app.config['CELERY_BROKER_URL'])
celery.conf.update(app.config)
# Data Model
class User(db.Model):
    __tablename__ = 'users'
    id = db.Column(db.Integer, primary_key=True)
    fname = db.Column(db.String(225))
    lname = db.Column(db.String(225))
    email = db.Column(db.String(225), unique=True)
    def __init__(self, fname, lname, email) -> None:
        super().__init__()
        self.fname = fname
        self.lname = lname
        self.email = email


@celery.task
def create_new_user(email):
    with app.app_context():
        user = User(fname="User", lname="User", email=email)
        db.session.add(user)
        try:
            db.session.commit()
            return {'status':'Created'}
        except Exception as e:
            db.session.rollback()
            db.session.flush()
            return {'status': 'Failed'}
    
@app.route('/', methods=['GET', 'POST'])
def register():
    message = ""
    if request.method == 'GET':
        return render_template('index.html')
    else:
        fname = request.form['fname']
        lname = request.form['lname']
        email = request.form['email']

        user = User(fname, lname, email)
        db.session.add(user)
        try:
            db.session.commit()
            message ="Successed!"
        except Exception as e:
            db.session.rollback()
            db.session.flush()
            message = "Failed!"
        
    return render_template('message.html', message=message)

@app.route('/search')
def search():
    query = request.args['query'].strip()
    if query != cache.get('query'):
        userResult = db.session.query(User).filter(User.email == query)
        if userResult.count() == 0:
            respose = create_new_user.apply_async(args=[query]).get()
            if respose['status'] == 'Created':
                return make_response(jsonify(respose), 202)
            else:
                return make_response(jsonify(respose), 402)
        else:
            user = userResult.first()
            cache.set('query', query)
            cache.set('user', user)
            data = {'user': {'fname': user.fname, 'lname': user.lname, 'email': user.email}}
            return make_response(jsonify(data), 200)
    else:
        user = cache.get('user')
        data = {'user': {'fname': user.fname, 'lname': user.lname, 'email': user.email}}
        return make_response(jsonify(data), 200)



if __name__ == '__main__':
    app.run(debug=True)
