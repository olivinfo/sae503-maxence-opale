import os, csv
from flask import Flask, request, jsonify
from redis import Redis
from flasgger import Swagger
from functools import wraps

app = Flask(__name__)
swagger = Swagger(app)

REDIS_HOST = os.getenv("REDIS_HOST", "redis-service")
REDIS_PORT = int(os.getenv("REDIS_PORT", 6379))
ADMIN_KEY = os.getenv("ADMIN_KEY", "default_key")
CSV_FILE_QUOTES = "initial_data_quotes.csv"

redis_client = Redis(host=REDIS_HOST, port=REDIS_PORT, db=0, decode_responses=True)

def authentification(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        auth_key = request.headers.get("Authorization")
        if not auth_key or auth_key != ADMIN_KEY:
            return jsonify({"error": "Unauthorized"}), 401
        return f(*args, **kwargs)
    return decorated

@app.avant_premiere_requete
def chargement_de_donnees():
    if not redis_client.exists("quotes"):
        if os.path.exists(CSV_FILE_QUOTES):
            with open(CSV_FILE_QUOTES, mode='r', encoding='utf-8') as file:
                reader = csv.DictReader(file)
                for row in reader:
                    qid = redis_client.incr("quote_id")
                    redis_client.hset(f"quotes:{qid}", mapping={"quote": row['quote']})
                    redis_client.sadd("quotes", f"quotes:{qid}")

@app.route('/quotes', methods=['GET'])
def get_quotes():
    quotes = [redis_client.hgetall(qid) for qid in redis_client.smembers("quotes")]
    return jsonify(quotes), 200

@app.route('/quotes', methods=['POST'])
@authentification
def add_quote():
    data = request.get_json()
    qid = redis_client.incr("quote_id")
    redis_client.hset(f"quotes:{qid}", mapping={"quote": data.get("quote")})
    redis_client.sadd("quotes", f"quotes:{qid}")
    return jsonify({"message": "Citation ajoutée", "id": qid}), 201

@app.route('/quotes/<int:quote_id>', methods=['DELETE'])
@authentification
def delete_quote(quote_id):
    if redis_client.exists(f"quotes:{quote_id}"):
        redis_client.delete(f"quotes:{quote_id}")
        redis_client.srem("quotes", f"quotes:{quote_id}")
        return jsonify({"message": "Supprimée"}), 200
    return jsonify({"error": "Non trouvée"}), 404

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=int(os.getenv("APP_PORT", 5000)))
