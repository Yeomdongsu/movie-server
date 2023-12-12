from flask import request
from flask_jwt_extended import get_jwt_identity, jwt_required
from flask_restful import Resource
from mysql_connection import get_connection
from mysql.connector import Error

# 영화 리뷰     
class MovieReviewResource(Resource) :
    # 해당 영화 리뷰 정보
    @jwt_required()
    def get(self, movie_id) :
        
        offset = request.args.get("offset")
        limit = request.args.get("limit")

        try :
            connection = get_connection()

            query = '''
                    select r.id, r.movieId, u.nickname, u.gender, r.rating
                    from review r
                    left join user u
                    on r.userId = u.id
                    where r.movieId = %s
                    limit ''' + offset + ''', ''' + limit + ''';
                    '''
            record = (movie_id, )
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
            return {"error" : "해당 영화는 리뷰가 없습니다."}, 400

        return {"result" : "success", "items" : result_list, "count" : len(result_list)}, 200
    
    # 해당 영화 리뷰 쓰기
    @jwt_required()
    def post(self, movie_id) :
        
        data = request.get_json()

        user_id = get_jwt_identity()

        try :
            connection = get_connection()

            query = '''
                    insert into review
                    (movieId, userId, rating, content)
                    values
                    (%s, %s, %s, %s);
                    '''
            record = (movie_id, user_id, data["rating"], data["content"])
            cursor = connection.cursor()
            cursor.execute(query, record)
            connection.commit()

            cursor.close()
            connection.close()

        except Error as e :
            print(e)
            cursor.close()
            connection.close()
            return {"result" : "fail", "error" : str(e)}, 500

        return {"result" : "success"}, 200
    