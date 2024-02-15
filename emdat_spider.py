# get emdat data
# https://public.emdat.be
import requests
import json
import datetime
import pandas as pd



def get_emdat_data():

    # current year
    l_year = datetime.datetime.today().year

    output_path = 'data/test.xlsx'

    # request url
    url = 'https://public.emdat.be/graphql/'

    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
                             'AppleWebKit/537.36 (KHTML, like Gecko) '
                             'Chrome/87.0.4280.67 Safari/537.36 Edg/87.0.664.47'}

    # username password for login
    login_data1 = {"operationName": "login",
            "variables": {"username": "1469503370@qq.com",
                          "password": "BNUZ2022bnuz"},
            "query": "mutation login($username: String!, $password: String!) "
                     "{\n  login(username: $username, password: $password)\n}"}

    # login post step2 data
    login_data2 = {"operationName": "self", "variables": {},
            "query": "query self {\n  self {\n    user_id\n    access {\n      "
                     "access_case\n      login\n      public\n      entry\n      "
                     "review\n      edit\n      admin\n    }\n    group {\n      "
                     "user_group_id\n      user_group\n      need_license\n    }\n    "
                     "roles {\n      role_ids\n      public\n    }\n    api_key\n  }\n}"}

    # login post step 1
    mys = requests.session()
    mys.post(url, headers=headers, data=json.dumps(login_data1))

    # login post step2
    mys.post(url, headers=headers, data=json.dumps(login_data2))

    # make search data
    se_data = {"operationName": "xlsx", "variables": {
        "form": {"period": [2017, l_year], "from": 2017, "include_hist": False, "classifications_tree": ["nat-*"],
                 "classif": ["nat-*"]}},
             "query": "mutation xlsx($form: PublicForm) {\n  get_public_xlsx(form: $form) {\n    public_version\n    count\n    link\n  }\n}"}

    # post search data get today's data url
    td_resp = mys.post(url, headers=headers, data=json.dumps(se_data))
    td_url = td_resp.json()['data']['get_public_xlsx']['link']

    # download today's data
    resp = mys.get(url=td_url, stream=True)
    with open(output_path, mode='wb') as f:
        for chunk in resp.iter_content(512):
            f.write(chunk)

    # data update check
    lastno = pd.read_excel('data/test.xlsx')['DisNo.'].iloc[-1]
    with open('data/lastNo.inf', 'w+', encoding='utf') as f:
        if f.read() != lastno:
            # update database
            f.write(lastno)
        else:
            pass



if __name__ == '__main__':
    get_emdat_data()






