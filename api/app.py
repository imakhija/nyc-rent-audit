from flask import Flask, request, jsonify, render_template
from core.predict import predict_all
from threading import Thread
from scripts.refresh_pipeline import run_pipeline
from scripts.fetch_listings import can_fetch

app = Flask(__name__)

@app.route("/")
def home():
    return render_template("home.html")

@app.route("/predict", methods=["POST"])
def predict():
    data = request.get_json()

    required_fields = ["bedrooms", "bathrooms", "zipCode", "rent"]
    missing = [f for f in required_fields if f not in data]
    if missing:
        return jsonify({"Bad request": f"Missing fields: {missing}"}), 400
    
    if can_fetch():
        Thread(target=run_pipeline).start()
    
    try:
        result = predict_all(data)
        return jsonify(result)
    except Exception as e:
        return jsonify({
            "error": "Inference failed",
            "details": str(e)
        }), 500

if __name__ == "__main__":
    app.run(debug=True)