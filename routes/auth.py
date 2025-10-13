from flask import Blueprint, request, jsonify 
from flask_restx import Namespace, Resource, fields
from extensions import db, bcrypt 
from models import User
from flask_jwt_extended import create_access_token, jwt_required, get_jwt_identity
from datetime import timedelta

auth_bp = Blueprint('auth', __name__)
auth_ns = Namespace('auth', description='User authentication operations')

# Define JWT expiration time
ACCESS_EXPIRES = timedelta(hours=24)

# Models for API documentation (Flask-RESTX)
user_model = auth_ns.model('User', {
    'username': fields.String(required=True, description='The user unique username'),
    'email': fields.String(description='The user email address'),
    'created_at': fields.DateTime(description='The date and time the user was created')
})

register_parser = auth_ns.parser()
register_parser.add_argument('username', type=str, required=True, help='Username', location='json')
register_parser.add_argument('password', type=str, required=True, help='Password', location='json')
register_parser.add_argument('email', type=str, required=False, help='Email address', location='json')

login_parser = auth_ns.parser()
login_parser.add_argument('username', type=str, required=True, help='Username', location='json')
login_parser.add_argument('password', type=str, required=True, help='Password', location='json')

@auth_ns.route('/register')
class UserRegister(Resource):
    @auth_ns.expect(register_parser, validate=True)
    @auth_ns.response(201, 'User successfully created', user_model)
    @auth_ns.response(409, 'Username or Email already exists')
    @auth_ns.response(400, 'Validation Error')
    def post(self):
        """Register a new user"""
        data = register_parser.parse_args()
        username = data['username']
        password = data['password']
        email = data.get('email')

        if User.query.filter_by(username=username).first():
            auth_ns.abort(409, "Username already exists")
        if email and User.query.filter_by(email=email).first():
            auth_ns.abort(409, "Email already exists")

        # Access bcrypt directly from import (it's initialized in main.py)
        hashed_password = bcrypt.generate_password_hash(password).decode('utf-8')
        new_user = User(username=username, hashed_password=hashed_password, email=email)
        db.session.add(new_user)
        db.session.commit()
        return {"message": "User registered successfully"}, 201

@auth_ns.route('/login')
class UserLogin(Resource):
    @auth_ns.expect(login_parser, validate=True)
    @auth_ns.response(200, 'Login successful', auth_ns.model('LoginSuccess', {'access_token': fields.String}))
    @auth_ns.response(401, 'Invalid credentials')
    @auth_ns.response(400, 'Validation Error')
    def post(self):
        """Login a user and return an access token"""
        data = login_parser.parse_args()
        username = data['username']
        password = data['password']

        user = User.query.filter_by(username=username).first()
        # Access bcrypt directly from import
        if user and bcrypt.check_password_hash(user.hashed_password, password):
            # Convert user.id to string before creating the token
            access_token = create_access_token(identity=str(user.id), expires_delta=ACCESS_EXPIRES) 
            return {"access_token": access_token}, 200
        else:
            auth_ns.abort(401, "Invalid username or password")

@auth_ns.route('/protected')
class ProtectedResource(Resource):
    @jwt_required()
    @auth_ns.response(200, 'Access granted')
    @auth_ns.response(401, 'Unauthorized')
    @auth_ns.doc(security='Bearer Auth')
    def get(self):
        """Access a protected resource (requires JWT token)"""
        current_user_id = get_jwt_identity()
        user = User.query.get(current_user_id)
        return {"logged_in_as": user.username}, 200
