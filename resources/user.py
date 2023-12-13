from flask import request
from flask_jwt_extended import create_access_token, get_jwt, get_jwt_identity, jwt_required
from flask_restful import Resource
from mysql_connection import get_connection
from mysql.connector import Error
from utils import hash_password, check_password
from email_validator import validate_email, EmailNotValidError

# 회원가입
class UserRegisterResource(Resource) :
    
    def post(self) :

        data = request.get_json()

        try :
            validate_email(data["email"])
        except EmailNotValidError as e :
            print(e)
            return {"error" : str(e)}, 400
        
        if len(data["password"]) < 4 or len(data["password"]) > 14 :
            return {"error" : "비밀번호 길이가 올바르지 않습니다."}, 400

        password = hash_password(data["password"])

        try :
            connection = get_connection()

            query = '''
                    insert into user
                    (email, password, nickname, gender)
                    values
                    (%s, %s, %s, %s);
                    '''
            record = (data["email"], password, data["nickname"], data["gender"])

            cursor = connection.cursor()
            cursor.execute(query, record)
            connection.commit()

            user_id = cursor.lastrowid

            cursor.close()
            connection.close()

        except Error as e :
            print(e)
            cursor.close()
            connection.close()
            return {"result" : "fail", "error" : str(e)}, 400

        access_token = create_access_token(user_id)

        return {"result" : "success", "access_token" : access_token}, 200 

# 로그인
class UserLoginResource(Resource) :
    
    def post(self) :

        data = request.get_json()

        try :
            connection = get_connection()

            query = '''
                    select * 
                    from user
                    where email = %s;
                    '''
            record = (data["email"], )

            cursor = connection.cursor(dictionary=True)
            cursor.execute(query, record)

            result_list = cursor.fetchall()

            cursor.close()
            connection.close()  

        except Error as e :
            print(e)
            cursor.close()
            connection.close()
            return {"result" : "fail", "error" : str(e)}, 500
        
        if len(result_list) == 0 :
            return {"error" : "등록된 회원이 아닙니다."}, 400
        
        check = check_password(data["password"], result_list[0]["password"])

        # 비밀번호가 안맞을 때
        if check == False :
            return {"error" : "비밀번호가 틀렸습니다."}, 400
        
        # JWT 인증 토큰 발급
        access_token = create_access_token(result_list[0]["id"])
        
        return  {"result" : "success", "access_token" : access_token}, 200

# 로그아웃
jwt_blocklist = set()
class UserLogoutResourcce(Resource) :

    @jwt_required()
    def delete(self) :
        
        jti = get_jwt()["jti"]

        jwt_blocklist.add(jti)

        return {"result" : "success"}, 200

# 내 정보 화면(내가 작성한 리뷰까지)
class UserInfoResource(Resource) :
    @jwt_required()
    def get(self) :

        user_id = get_jwt_identity()

        offset = request.args.get("offset")
        limit = request.args.get("limit")

        try :
            connection = get_connection()

            query = '''
                    select u.email, u.nickname, u.gender, m.title, r.rating
                    from user u
                    join review r
                    on u.id = r.userId
                    join movie m
                    on r.movieId = m.id
                    where u.id = %s
                    order by r.createdAt desc
                    limit ''' + offset + ''', ''' + limit + ''';
                    '''
            record = (user_id, )
            cursor = connection.cursor(dictionary=True)
            cursor.execute(query, record)

            result_list = cursor.fetchall()

            cursor.close()
            connection.close()

        except Error as e :
            print(e)
            cursor.close()
            connection.close()
            return {"result" : "fail", "error" : str(e)}, 500
        
        return {"result" : "success", "items" : result_list, "count" : len(result_list)}, 200
