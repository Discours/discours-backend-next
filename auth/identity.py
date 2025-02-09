from binascii import hexlify
from hashlib import sha256

# from base.exceptions import InvalidPassword, InvalidToken
from services.db import local_session
from auth.exceptions import ExpiredToken, InvalidToken
from passlib.hash import bcrypt

from auth.jwtcodec import JWTCodec
from auth.tokenstorage import TokenStorage
from orm.user import User


class Password:
    @staticmethod
    def _to_bytes(data: str) -> bytes:
        return bytes(data.encode())

    @classmethod
    def _get_sha256(cls, password: str) -> bytes:
        bytes_password = cls._to_bytes(password)
        return hexlify(sha256(bytes_password).digest())

    @staticmethod
    def encode(password: str) -> str:
        password_sha256 = Password._get_sha256(password)
        return bcrypt.using(rounds=10).hash(password_sha256)

    @staticmethod
    def verify(password: str, hashed: str) -> bool:
        """
        Verify that password hash is equal to specified hash. Hash format:

        $2a$10$Ro0CUfOqk6cXEKf3dyaM7OhSCvnwM9s4wIX9JeLapehKK5YdLxKcm
        \__/\/ \____________________/\_____________________________/  # noqa: W605
        |   |        Salt                     Hash
        |  Cost
        Version

        More info: https://passlib.readthedocs.io/en/stable/lib/passlib.hash.bcrypt.html

        :param password: clear text password
        :param hashed: hash of the password
        :return: True if clear text password matches specified hash
        """
        hashed_bytes = Password._to_bytes(hashed)
        password_sha256 = Password._get_sha256(password)

        return bcrypt.verify(password_sha256, hashed_bytes)


class Identity:
    @staticmethod
    def password(orm_user: User, password: str) -> User:
        user = User(**orm_user.dict())
        if not user.password:
            # raise InvalidPassword("User password is empty")
            return {"error": "User password is empty"}
        if not Password.verify(password, user.password):
            # raise InvalidPassword("Wrong user password")
            return {"error": "Wrong user password"}
        return user

    @staticmethod
    def oauth(inp) -> User:
        with local_session() as session:
            user = session.query(User).filter(User.email == inp["email"]).first()
            if not user:
                user = User.create(**inp, emailConfirmed=True)
                session.commit()

        return user

    @staticmethod
    async def onetime(token: str) -> User:
        try:
            print("[auth.identity] using one time token")
            payload = JWTCodec.decode(token)
            if not await TokenStorage.exist(f"{payload.user_id}-{payload.username}-{token}"):
                # raise InvalidToken("Login token has expired, please login again")
                return {"error": "Token has expired"}
        except ExpiredToken:
            # raise InvalidToken("Login token has expired, please try again")
            return {"error": "Token has expired"}
        except InvalidToken:
            # raise InvalidToken("token format error") from e
            return {"error": "Token format error"}
        with local_session() as session:
            user = session.query(User).filter_by(id=payload.user_id).first()
            if not user:
                # raise Exception("user not exist")
                return {"error": "User does not exist"}
            if not user.emailConfirmed:
                user.emailConfirmed = True
                session.commit()
            return user
