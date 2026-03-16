from fastembed import TextEmbedding
from flask import Flask, request, jsonify

app = Flask(__name__)
model = TextEmbedding("sentence-transformers/all-MiniLM-L6-v2")


@app.route("/embed", methods=["POST"])
def embed():
    texts = request.json["texts"]
    embeddings = [e.tolist() for e in model.embed(texts)]
    return jsonify({"embeddings": embeddings})


@app.route("/health")
def health():
    return "OK"


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=9000)
