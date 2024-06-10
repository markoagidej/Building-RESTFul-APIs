# Task 1: Setting Up the Flask Environment and Database Connection
from flask import Flask, jsonify, request
from flask_marshmallow import Marshmallow
from marshmallow import fields, ValidationError
import mysql.connector
from mysql.connector import Error
from my_password import password as my_password

app = Flask(__name__)
ma = Marshmallow(app)

#Defining Schemas
class MemberSchema(ma.Schema):
    name = fields.String(required=True)
    age = fields.Integer(required=True)
    trainer_id = fields.String(required=True)

    class Meta:
        fields = ("name", "age", "trainer_id", "id")

class WorkoutSessionSchema(ma.Schema):
    member_id = fields.Integer(required=True)
    date = fields.Date(required=True)
    duration_minutes = fields.Integer(required=True)
    calories_burned = fields.Integer(required=True)

    class Meta:
        fields = ("member_id", "date", "duration_minutes", "calories_burned", "id")

# Instance creation of Schemas
memberSchema = MemberSchema()
membersSchema = MemberSchema(many=True)

workoutSessionSchema = WorkoutSessionSchema()
workoutSessionsSchema = WorkoutSessionSchema(many=True)

# Connecting DB
def get_db_connection():
    db_name = "applying_sql_in_python"
    user = "root"
    password = my_password
    host = "localhost"

    try:
        conn = mysql.connector.connect(
            database=db_name,
            user=user,
            password=password,
            host=host
        )

        print("Connected to DB")
        return conn
    except Error as e:
        print(f"Error: {e}")
        return None

# Task 2: Implementing CRUD Operations for Members
@app.route('/members', methods=['POST'])
def add_member():
    try:
        member_data = memberSchema.load(request.json)
    except ValidationError as e:
        print(f"Error: {e}")
        return jsonify(e.messages), 400
    
    try:
        conn = get_db_connection()
        if conn is None:
            return jsonify({"error": "Database connection failed"}), 500
        cursor = conn.cursor()
        new_member = (member_data['name'], member_data['age'], member_data['trainer_id'])
        query = "INSERT INTO customers (name, age, trainer_id) VALUES (%s, %s, %s)"
        cursor.execute(query, new_member)
        conn.commit()
        return jsonify({"message": "New member added sucesfully"}), 201
    except Error as e:
        print(f"Error: {e}")
        return jsonify({"error": "Internal server error"}), 500
    finally:
        if conn and conn.is_connected():
            cursor.close()
            conn.close()

@app.route('/members/<int:id>', methods=['GET'])
def get_member(id):
    try:
        conn = get_db_connection()
        if conn is None:
            return jsonify({"error": "Database connection failed"}), 500        
        cursor = conn.cursor(dictionary = True)
        query = "SELECT * FROM Members"
        cursor.execute(query)
        customers = cursor.fetchall()
        return membersSchema.jsonify(customers)
    except Error as e:
        print(f"Error: {e}")
        return jsonify({"error": "Internal server error"}), 500
    finally:
        if conn and conn.is_connected():
            cursor.close()
            conn.close()    

@app.route('/members', methods=['PUT'])
def add_member():
    try:
        member_data = memberSchema.load(request.json)
    except ValidationError as e:
        print(f"Error: {e}")
        return jsonify(e.messages), 400
    
    try:
        conn = get_db_connection()
        if conn is None:
            return jsonify({"error": "Database connection failed"}), 500
        cursor = conn.cursor()

        updated_member = (member_data['name'], member_data['age'], member_data['trainer_id'], id)
        query = "UPDATE members SET name = %s, age = %s, trainer_id = %s WHERE id = %s"
        cursor.execute(query, updated_member)
        conn.commit()

        return jsonify({"message": "Member updated sucesfully"}), 201
    
    except Error as e:
        print(f"Error: {e}")
        return jsonify({"error": "Internal server error"}), 500
    finally:
        if conn and conn.is_connected():
            cursor.close()
            conn.close()

@app.route('/members/<int:id>', methods=['DELETE'])
def get_member(id): 
    try:
        conn = get_db_connection()
        if conn is None:
            return jsonify({"error": "Database connection failed"}), 500
        cursor = conn.cursor()

        member_to_remove = (id,)

        cursor.execute("SELECT * FROM members WHERE id = %s", member_to_remove)
        member = cursor.fetchone()
        if not member:
            return jsonify({"error": "Member not found"}), 404

        query = "DELETE FROM members WHERE id = %s"
        cursor.execute(query, member_to_remove)
        conn.commit()

        return jsonify({"message": "Member deleted sucesfully"}), 200
    
    except Error as e:
        print(f"Error: {e}")
        return jsonify({"error": "Internal server error"}), 500
    finally:
        if conn and conn.is_connected():
            cursor.close()
            conn.close()

# Task 3: Managing Workout Sessions