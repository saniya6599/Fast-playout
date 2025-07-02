from flask import Flask, request, jsonify
from flask_httpauth import HTTPBasicAuth
from werkzeug.security import generate_password_hash, check_password_hash

# Initialize the Flask application and the HTTPBasicAuth object
app = Flask(__name__)
auth = HTTPBasicAuth()

# Dummy user database for authentication
users = {
    "admin": generate_password_hash("secret")
}

# Authentication verification function
@auth.verify_password
def verify_password(username, password):
    if username in users and check_password_hash(users.get(username), password):
        return username
    return None

# Example module functions
def module_one():
    return "Module One Executed"

def module_two():
    return "Module Two Executed"

# Command handler endpoint
@app.route('/command', methods=['POST'])
@auth.login_required
def handle_command():
    data = request.json
    command_id = data.get('command_id')
    
    if not command_id:
        return jsonify({"error": "Invalid request, command_id is required"}), 400

    # Command handling logic
    if command_id == 1:
        result = module_one()
    elif command_id == 2:
        result = module_two()
    else:
        result = "Unknown command"

    return jsonify({"result": result})

# Main entry point
if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)
