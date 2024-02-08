from flask import Flask, jsonify, request, current_app
from flask_sqlalchemy import SQLAlchemy
from flask_marshmallow import Marshmallow
from flask_mysqldb import MySQL
from flask_cors import CORS
from sqlalchemy.ext.automap import automap_base
from sqlalchemy import text, create_engine

app = Flask(__name__)
CORS(app)

app.config['SQLALCHEMY_DATABASE_URI'] = 'mysql://root:password@localhost/sakila'
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False

app.config['MYSQL_USER'] = 'root'
app.config['MYSQL_PASSWORD'] = 'password'
app.config['MYSQL_HOST'] = 'localhost'
app.config['MYSQL_DB'] = 'sakila'
app.config['MYSQL_CURSORCLASS'] = 'DictCursor'


db = SQLAlchemy(app)
ma = Marshmallow(app)
mysql = MySQL(app)
app.app_context().push()


actor = db.Table('actor', db.metadata, autoload_with = db.engine)
address = db.Table('address', db.metadata, autoload_with = db.engine)
category = db.Table('category', db.metadata, autoload_with = db.engine)
city = db.Table('city', db.metadata, autoload_with = db.engine)
country = db.Table('country', db.metadata, autoload_with = db.engine)
customer = db.Table('customer', db.metadata, autoload_with = db.engine)
film = db.Table('film', db.metadata, autoload_with = db.engine)
film_actor = db.Table('film_actor', db.metadata, autoload_with = db.engine)
film_text = db.Table('film_text', db.metadata, autoload_with = db.engine)
inventory = db.Table('inventory', db.metadata, autoload_with = db.engine)
language = db.Table('language', db.metadata, autoload_with = db.engine)
payment = db.Table('payment', db.metadata, autoload_with = db.engine)
rental = db.Table('rental', db.metadata, autoload_with = db.engine)
staff = db.Table('staff', db.metadata, autoload_with = db.engine)
store = db.Table('store', db.metadata, autoload_with = db.engine)

class filmSchema(ma.Schema):
    class Meta:
        fields = ('film_id','title','name', 'description', 'release_year', 'length', 'rating')

class actorSchema(ma.Schema):
    class Meta:
        fields = ('actor_id','first_name','last_name')

class actorMovieSchema(ma.Schema):
    class Meta:
        fields = ('film_id', 'title')

class allFilmsSchema(ma.Schema):
    class Meta:
        fields = ('FID','title', 'category', 'length', 'rating', 'actors','price')

class customersSchema(ma.Schema):
    class Meta:
        fields = ('ID','name')

customer_schema = customersSchema()
customers_schema = customersSchema(many = True)

film_schema = filmSchema()
films_schema = filmSchema(many = True)

actor_schema = actorSchema()
actors_schema = actorSchema(many = True)

actorMovie_schema = actorMovieSchema()
actorMovies_schema = actorMovieSchema(many = True)

allFilm_schema = allFilmsSchema()
allFilms_schema = allFilmsSchema(many = True)

engine = create_engine('mysql://root:password@localhost/sakila')
conn = engine.connect()

film_text = text("select film.film_id, film.title, category.name, film.description, film.release_year, film.length, film.rating, count(rental.inventory_id) as rented from rental join inventory on inventory.inventory_id = rental.inventory_id join film on inventory.film_id = film.film_id join film_category on film_category.film_id = film.film_id join category on category.category_id = film_category.category_id group by film.film_id, name order by rented desc, title limit 5;")
actor_text = text("select actor.actor_id, first_name, last_name, count(*) as movies from actor join film_actor on actor.actor_id = film_actor.actor_id group by actor_id order by movies desc limit 5;")

num1_actor_text = text("select film.film_id, film.title, count(rental.inventory_id) as rented from rental join inventory on inventory.inventory_id = rental.inventory_id join film on inventory.film_id = film.film_id join film_actor on film_actor.film_id = film.film_id join actor on film_actor.actor_id = actor.actor_id where actor.first_name = 'Gina' group by film.film_id, title having count(*) order by rented desc, title limit 5;")
num2_actor_text = text("select film.film_id, film.title, count(rental.inventory_id) as rented from rental join inventory on inventory.inventory_id = rental.inventory_id join film on inventory.film_id = film.film_id join film_actor on film_actor.film_id = film.film_id join actor on film_actor.actor_id = actor.actor_id where actor.first_name = 'Walter' group by film.film_id, title having count(*) order by rented desc, title limit 5;")
num3_actor_text = text("select film.film_id, film.title, count(rental.inventory_id) as rented from rental join inventory on inventory.inventory_id = rental.inventory_id join film on inventory.film_id = film.film_id join film_actor on film_actor.film_id = film.film_id join actor on film_actor.actor_id = actor.actor_id where actor.first_name = 'Mary' group by film.film_id, title having count(*) order by rented desc, title limit 5;")
num4_actor_text = text("select film.film_id, film.title, count(rental.inventory_id) as rented from rental join inventory on inventory.inventory_id = rental.inventory_id join film on inventory.film_id = film.film_id join film_actor on film_actor.film_id = film.film_id join actor on film_actor.actor_id = actor.actor_id where actor.first_name = 'Matthew' group by film.film_id, title having count(*) order by rented desc, title limit 5;")
num5_actor_text = text("select film.film_id, film.title, count(rental.inventory_id) as rented from rental join inventory on inventory.inventory_id = rental.inventory_id join film on inventory.film_id = film.film_id join film_actor on film_actor.film_id = film.film_id join actor on film_actor.actor_id = actor.actor_id where actor.first_name = 'Sandra' group by film.film_id, title having count(*) order by rented desc, title limit 5;")

all_Films_text = text("SELECT * FROM sakila.nicer_but_slower_film_list")

customer_text = text("SELECT * FROM sakila.customer_list;")

@app.route('/customers', methods = ['GET'])
def Customer():
    result = conn.execute(customer_text).fetchall()
    results = customers_schema.dump(result)
    return jsonify(results)


@app.route('/films', methods = ['GET'])
def index():
    result = conn.execute(film_text).fetchall()
    results = films_schema.dump(result)
    for r in results:
        print(r)
    return jsonify(results)

@app.route('/allFilms', methods = ['GET'])
def FilmSearch():
    result = conn.execute(all_Films_text).fetchall()
    results = allFilms_schema.dump(result)
    return jsonify(results)


@app.route('/actors', methods = ['GET'])
def index2():
    result = conn.execute(actor_text).fetchall()
    results = actors_schema.dump(result)
    for r in results:
        print(r)
    return jsonify(results)

@app.route('/num1movie', methods = ['GET'])
def index3():
    result = conn.execute(num1_actor_text).fetchall()
    results = actorMovies_schema.dump(result)
    for r in results:
        print(r)
    return jsonify(results)

@app.route('/num2movie', methods = ['GET'])
def index4():
    result = conn.execute(num2_actor_text).fetchall()
    results = actorMovies_schema.dump(result)
    for r in results:
        print(r)
    return jsonify(results)

@app.route('/num3movie', methods = ['GET'])
def index5():
    result = conn.execute(num3_actor_text).fetchall()
    results = actorMovies_schema.dump(result)
    for r in results:
        print(r)
    return jsonify(results)

@app.route('/num4movie', methods = ['GET'])
def index6():
    result = conn.execute(num4_actor_text).fetchall()
    results = actorMovies_schema.dump(result)
    for r in results:
        print(r)
    return jsonify(results)

@app.route('/num5movie', methods = ['GET'])
def index7():
    result = conn.execute(num5_actor_text).fetchall()
    results = actorMovies_schema.dump(result)
    for r in results:
        print(r)
    return jsonify(results)


if __name__ == "__main__":
    app.run(debug=True)