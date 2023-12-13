from flask import request
from flask_jwt_extended import get_jwt_identity, jwt_required
from flask_restful import Resource
from mysql_connection import get_connection
from mysql.connector import Error

# 즐겨찾기
class FavoriteResource(Resource) :
    # 즐겨찾기 추가
    @jwt_required()
    def post(self) :

        user_id = get_jwt_identity()

        movie = request.args.get("movie")

        try :
            connection = get_connection()

            query = '''
                    insert into favorite
                    (movieId, userId)
                    values
                    (%s, %s);
                    '''
            
            record = (movie, user_id)
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
    
    # 즐겨찾기 삭제
    @jwt_required()
    def delete(self) :

        user_id = get_jwt_identity()

        movie = request.args.get("movie")

        try :
            connection = get_connection()

            query = '''
                    delete from favorite
                    where movieId = %s and userId = %s;
                    '''
            record = (movie, user_id)
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
    
# 즐겨찾기 한 영화만 보여주기
class FavoriteMovieResource(Resource) :
    
    @jwt_required()
    def get(self) :

        user_id = get_jwt_identity()

        offset = request.args.get("offset")
        limit = request.args.get("limit")

        try :
            connection = get_connection()

            query = '''
                    select m.id, m.title, m.genre, m.year, m.attendance, f.userId 
                    from movie m
                    join favorite f
                    on m.id = f.movieId
                    where f.userId = %s
                    limit ''' + offset + ''', ''' + limit + ''';
                    '''
            record = (user_id, )
            cursor = connection.cursor(dictionary=True)
            cursor.execute(query, record)

            result_list = cursor.fetchall()

            i = 0
            for row in result_list :
                result_list[i]["year"] = row["year"].isoformat()
                i = i+1

            cursor.close()
            connection.close()

        except Error as e :
            print(e)
            cursor.close()
            connection.close()
            return {"result" : "fail", "error" : str(e)}, 500

        return {"result" : "success", "items" : result_list, "count" : len(result_list)}, 200