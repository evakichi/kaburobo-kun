import requests
import os
import json


class TokenTaker:

    def __init__(self) -> None:
        self.user = os.environ.get('J_QUANTS_USER')
        self.passwd = os.environ.get('J_QUANTS_PASSWD')

    def get_token(self) -> str:

        data = {"mailaddress": self.user, "password": self.passwd}
        refresh_token_taker = requests.post(
            "https://api.jquants.com/v1/token/auth_user", data=json.dumps(data))
        refresh_token = refresh_token_taker.json()['refreshToken']

        id_token_taker = requests.post(
            f"https://api.jquants.com/v1/token/auth_refresh?refreshtoken={refresh_token}")
        id_token = id_token_taker.json()['idToken']

        return id_token

class BrandTaker:

    def __init__(self,id_token):
        self.id_token = id_token
        self.info = self.get_brand()

    def get_brand(self):
        headers = {'Authorization': f"Bearer {self.id_token}"}
        requests_obtainer = requests.get(
            f"https://api.jquants.com/v1/listed/info", headers=headers)
        return requests_obtainer.json()['info']

if __name__=="__main__":
    token_taker = TokenTaker()
    brand_taker = BrandTaker(token_taker.get_token())
    for brand in brand_taker.info:
        print(brand)