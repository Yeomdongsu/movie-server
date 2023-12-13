from flask import request
from flask_jwt_extended import get_jwt_identity, jwt_required
from flask_restful import Resource
from mysql_connection import get_connection
from mysql.connector import Error

# 영화 리스트
class MovieListResource(Resource) :
    # 영화 정보를 보여주는 API 개발(리뷰개수 내림차순, 별점평균 내림차순)
    @jwt_required()
    def get(self) :
        
        order = request.args.get("order")
        offset = request.args.get("offset")
        limit = request.args.get("limit")

        user_id = get_jwt_identity()

        try :
            connection = get_connection()

            query = '''
                    select m.id, m.title, count(r.id) as reviewCnt, avg(r.rating) as avgRating, if(f.id is null, 0, 1) as isFavorite
                    from movie m
                    left join review r
                    on m.id = r.movieId
                    left join favorite f
                    on m.id = f.movieId and f.userId = %s
                    group by m.id
                    order by ''' + order + ''' desc
                    limit ''' + offset + ''', ''' + limit + ''';
                    '''
            record = (user_id, )    
            cursor = connection.cursor(dictionary=True)
            cursor.execute(query, record)

            result_list = cursor.fetchall()

            i = 0
            for row in result_list :
                result_list[i]["avgRating"] = float(row["avgRating"])
                i = i+1

            cursor.close()
            connection.close()

        except Error as e :
            print(e)
            cursor.close()
            connection.close()
            return {"result" : "fail", "error" : str(e)}, 500
        
        return {"result" : "success", "items" : result_list, "count" : len(result_list)}, 200
    
    # 영화 검색 API
    @jwt_required(optional=True)
    def post(self) :

        keyword = request.args.get("keyword")
        offset = request.args.get("offset")
        limit = request.args.get("limit")

        try :
            connection = get_connection()

            query = '''
                    select m.id, m.title, m.summary, count(r.id) as reviewCnt, ifnull(avg(r.rating), 0) as ratingAvg
                    from movie m
                    left join review r
                    on m.id = r.movieId
                    where m.title like "%''' + keyword + '''%" or m.summary like "%''' + keyword + '''%"
                    group by m.id
                    limit ''' + offset + ''', ''' + limit + ''';
                    '''
            
            cursor = connection.cursor(dictionary=True)
            cursor.execute(query)

            result_list = cursor.fetchall()

            i = 0
            for row in result_list :
                result_list[i]["ratingAvg"] = float(row["ratingAvg"])
                i = i+1

            cursor.close()
            connection.close()

        except Error as e :
            print(e)
            cursor.close()
            connection.close()
            return {"result" : "fail", "error" : str(e)}, 500
        
        if len(result_list) == 0 :
            return {"error" : "해당 영화가 존재하지 않습니다."}, 400

        return {"result" : "success", "items" : result_list, "count" : len(result_list)}, 200
    
# 영화 상세 정보
class MovieResource(Resource) :
    # 영화 상세 정보
    @jwt_required(optional=True)
    def get(self, movie_id) :
        
        try :
            connection = get_connection()

            query = '''
                    select m.id, m.title, m.summary, m.year, m.attendance, ifnull(avg(r.rating), 0) as review_avg, count(r.id) as review_cnt
                    from movie m
                    left join review r
                    on m.id = r.movieId
                    where m.id = %s
                    group by m.id;
                    '''
            record = (movie_id, )
            cursor = connection.cursor(dictionary=True)
            cursor.execute(query, record)

            result_list = cursor.fetchall()

            i = 0
            for row in result_list :
                result_list[i]["year"] = row["year"].isoformat()
                result_list[i]["review_avg"] = float(row["review_avg"])
                i = i+1

            cursor.close()
            connection.close()

        except Error as e : 
            print(e)
            cursor.close()
            connection.close()
            return {"result" : "fail", "error" : str(e)}, 500
        
        return {"result" : "success", "items" : result_list}, 200

