from flask import Flask
from flask_jwt_extended import JWTManager
from flask_restful import Api
from config import Config
from resources.favorite import FavoriteMovieResource, FavoriteResource
from resources.movie import MovieListResource, MovieResource
from resources.recommend import MovieRecommendResource
from resources.review import ReviewResource
from resources.user import UserInfoResource, UserLoginResource, UserLogoutResourcce, UserRegisterResource
# 로그아웃 관련된 import문
from resources.user import jwt_blocklist

# flask 프레임워크를 이용한 Restful API 서버 개발

app = Flask(__name__)

# 환경변수 세팅
app.config.from_object(Config)
# JWT 매니저를 초기화
jwt = JWTManager(app)
# 로그아웃된 토큰으로 요청하면, 실행되지 않게 처리하는 코드
@jwt.token_in_blocklist_loader
def check_if_token_is_revoked(jwt_header, jwt_payload) :
    jti = jwt_payload['jti']
    return jti in jwt_blocklist

api = Api(app)

# API를 구분해서 실행시키는 것은 HTTP method와 url의 조합이다.

# 리소스(API코드)와 경로를 연결한다.
api.add_resource(UserRegisterResource, "/user/register")
api.add_resource(UserLoginResource, "/user/login")
api.add_resource(UserLogoutResourcce, "/user/logout")
api.add_resource(MovieListResource, "/movie")
api.add_resource(MovieResource, "/movie/<int:movie_id>")
api.add_resource(ReviewResource, "/movie/<int:movie_id>/review")
api.add_resource(MovieRecommendResource, "/movie/recommend")
api.add_resource(UserInfoResource, "/user/info")
api.add_resource(FavoriteResource, "/favorite")
api.add_resource(FavoriteMovieResource, "/favorite/movie")

if __name__ == "__main__" :
    app.run()