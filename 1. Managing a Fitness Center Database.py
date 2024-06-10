# Task 1: Setting Up the Flask Environment and Database Connection
from flask import Flask, jsonify, request
from flask_marshmallow import Marshmallow
from marshmallow import fields
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
MemberSchema = MemberSchema()
MembersSchema = MemberSchema(many=True)

WorkoutSessionSchema = WorkoutSessionSchema()
WorkoutSessionsSchema = WorkoutSessionSchema(many=True)

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

# Task 3: Managing Workout Sessions