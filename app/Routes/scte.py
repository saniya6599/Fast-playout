# app/routes/scte.py

from flask import Blueprint, request, jsonify
from app.services.scteservice import ScteService


scte_blueprint = Blueprint('scte', __name__)

scte_service = ScteService()

@scte_blueprint.route("/scte/marker", methods=["POST"])
def create_scte_marker():
    """
    API endpoint to accept SCTE marker data.
    Returns:
        dict: Success or error message.
    """
    try:

        scte_data = request.get_json()

        if not scte_data:
            return jsonify({"status": "error", "message": "Invalid input, no data provided"}), 400


        response = scte_service.process_scte_marker(scte_data)
        
        return jsonify(response), 200

    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 400
