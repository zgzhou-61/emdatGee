import ee
import os
import requests
import json
import datetime
import pandas as pd
import pymysql
import configparser
from sqlalchemy import create_engine

class Emdat():
    def __init__(self):
        ee.Initialize()
        self.__cfg = configparser.ConfigParser()
        self.__BASE_DIR = os.path.dirname(os.path.abspath(__file__))
        self.__cfg.read(os.path.join(self.__BASE_DIR, 'emdatGee.ini'))
        self.__emdat_account = dict(self.__cfg.items('emdat_account'))
        self.__conSet = dict(self.__cfg.items('mysql_connset'))
        self.__mysql_attribute = dict(self.__cfg.items('mysql_attribute'))
        self.__emdat_table = self.__mysql_attribute['emdat_table']
        self.__lat8_bandsInfo_table = self.__mysql_attribute['lat8_bandsinfo_table']

        # connect mysql
        self.conn = pymysql.connect(
            host=self.__conSet['host'],
            port=int(self.__conSet['port']),
            user=self.__conSet['username'],
            password=self.__conSet['password'],
            db=self.__conSet['database'],
            charset=self.__conSet['charset']
        )

    def emdat_spider(self):
        # current year
        '''
        l_year = datetime.datetime.today().year

        output_path = 'data/test.xlsx'

        # request url
        url = 'https://public.emdat.be/graphql/'

        headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
                                 'AppleWebKit/537.36 (KHTML, like Gecko) '
                                 'Chrome/87.0.4280.67 Safari/537.36 Edg/87.0.664.47'}

        # username password for login
        login_data1 = {"operationName": "login",
                       "variables": {"username": self.__emdat_account['emdat_username'],
                                     "password": self.__emdat_account['emdat_password']},
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
        '''


        # data update check
        emdat_data = pd.read_excel('data/test.xlsx')
        lastno_cur = emdat_data['DisNo.'].iloc[-1]

        with open('data/lastNo.inf', 'r+', encoding='utf') as f:
            lastno = f.readline()
            if lastno != lastno_cur:
                # update database
                for i in range(-1, -len(emdat_data), -1):
                    item = emdat_data.iloc[i]
                    if item['DisNo.'] != lastno:
                        if pd.notnull(item['Latitude']):
                            engineInfo = '{0}+{1}://{2}:{3}@{4}:{5}/{6}?charset={7}'.format(
                                self.__conSet['dialect'],
                                self.__conSet['driver'],
                                self.__conSet['username'],
                                self.__conSet['password'],
                                self.__conSet['host'],
                                self.__conSet['port'],
                                self.__conSet['database'],
                                self.__conSet['charset']
                            )
                            conn = create_engine(engineInfo).connect()
                    else:
                        break
                # f.write(lastno)
            else:
                pass

    def getEmdatFromMysql(self):
        '''
        get emdat data(Points) from mysql, set mysql connect args from emdatGee.ini
        :return:emdat data(Points) dataframe
        '''

        emdat_headers = ['DisNo.', 'Latitude', 'Longitude', 'Start Year', 'Start Month',  'End Year', 'End Month']
        sql_condition = "WHERE em.Latitude != 'Null' AND em.Longitude != 'Null'"

        # headers to sql
        select_items = '`' + str(emdat_headers[0]) + '`'
        for i in emdat_headers[1:]:
            select_items = select_items + ', `' + i + '`'

        # set select sql
        cur = self.conn.cursor()
        cur.execute("SELECT" + select_items +
                    " FROM `" + self.__emdat_table + "` em " + sql_condition)
        # "WHERE em.Latitude != 'Null' AND em.Longitude != 'Null'")

        result = cur.fetchall()
        cur.close()
        return pd.DataFrame(result, columns=emdat_headers)

    def __timeSet(self, startYear, startMonth, endYear, endMonth):
        '''
        Get the time range of one year before and after the target time period
        :param startYear: start year
        :param startMonth: star month
        :param endYear: end year
        :param endMonth: end month
        :return: time limit
        '''

        if startMonth - 6 >= 1:
            startDate = '{0}-{1}-1'.format(startYear, startMonth - 6)
        else:
            startDate = '{0}-{1}-1'.format(startYear - 1, 12 + (startMonth - 6))

        if endMonth - 6 >= 1:
            endDate = '{0}-{1}-1'.format(endYear + 1, 0 + (endMonth - 6))
        else:
            endDate = '{0}-{1}-1'.format(endYear, endMonth + 6)

        return [startDate, endDate]

    def getPointSR_FromCollections(self, point_axis, bands, srcImgColletion, time, scale=30):
        '''
        get bands infomation of the taget point
        :param point_axis: axis of point
        :param bands: bands of satellite
        :param srcImgColletion: taget ImageCollection from gee
        :param time: time limit
        :param scale: scale of image
        :return: bands infomation of taget point
        '''

        # landsat8 cloud mask
        def maskL8sr(image):
            qaMask = image.select('QA_PIXEL').bitwiseAnd(37).eq(0)
            saturationMask = image.select('QA_RADSAT').eq(0)

            return image.updateMask(qaMask) \
                .updateMask(saturationMask)

        # Applies scaling factors.
        def apply_scale_factors(image):
            optical_bands = image.select('SR_B.').multiply(0.0000275).add(-0.2)
            thermal_bands = image.select('ST_B.*').multiply(0.00341802).add(149.0)
            return image.addBands(optical_bands, None, True).addBands(
                thermal_bands, None, True
            )

        def mask_s2_clouds(image):
            """Masks clouds in a Sentinel-2 image using the QA band.

            Args:
                image (ee.Image): A Sentinel-2 image.

            Returns:
                ee.Image: A cloud-masked Sentinel-2 image.
            """
            qa = image.select('QA60')

            # Bits 10 and 11 are clouds and cirrus, respectively.
            cloud_bit_mask = 1 << 10
            cirrus_bit_mask = 1 << 11

            # Both flags should be set to zero, indicating clear conditions.
            mask = (
                qa.bitwiseAnd(cloud_bit_mask)
                .eq(0)
                .And(qa.bitwiseAnd(cirrus_bit_mask).eq(0))
            )

            return image.updateMask(mask)

        # set imgCollection
        lat8Src = "LANDSAT/LC08/C02/T1_L2"
        # s2 can't use
        # s2Src = "COPERNICUS/S2_SR_HARMONIZED"

        if srcImgColletion == lat8Src:
            dataset = ee.ImageCollection(srcImgColletion) \
                .filterDate(time[0], time[1]) \
                .map(apply_scale_factors) \
                .map(maskL8sr)
        # elif srcImgColletion == s2Src:
        #     dataset = ee.ImageCollection(srcImgColletion) \
        #         .filterDate(time[0], time[1]) \
        #         .map(mask_s2_clouds)
        else:
            print("Wrong ImgCollection!")
            return


        # set roi
        roi = ee.Geometry.Point(list(point_axis))

        # get point sr
        geom_values = dataset.select(bands).getRegion(geometry=roi, scale=scale)
        geom_values_list = ee.List(geom_values).getInfo()
        header = geom_values_list[0]
        geom_df = pd.DataFrame(geom_values_list[1:], columns=header)
        geom_df['datetime'] = pd.to_datetime(geom_df['time'], unit='ms', utc=False)
        strTime = geom_df.datetime.map(lambda x: x.strftime('%Y-%m-%d'))
        geom_df['datetime'] = strTime
        geom_df = geom_df.drop('time', axis=1)

        return geom_df

    def initLat8ToDB(self):
        cur = self.conn.cursor()
        # create emdat_bandsInfo_table
        createTableSql = "CREATE TABLE IF NOT EXISTS " + self.__lat8_bandsInfo_table + "(" \
            "`DisNo.` varchar(255) ," \
            "FOREIGN KEY (`DisNo.`) REFERENCES `" + self.__emdat_table + "` (`DisNo.`)," \
            "id varchar(255)," \
            "SR_B1 DECIMAL(12, 10)," \
            "SR_B2 DECIMAL(12, 10)," \
            "SR_B3 DECIMAL(12, 10)," \
            "SR_B4 DECIMAL(12, 10)," \
            "SR_B5 DECIMAL(12, 10)," \
            "SR_B6 DECIMAL(12, 10)," \
            "SR_B7 DECIMAL(12, 10)," \
            "datetime DATE)"

        cur.execute(createTableSql)

        # bandInfoDate to BandsInfoDB
        eData = self.getEmdatFromMysql()
        engineInfo = '{0}+{1}://{2}:{3}@{4}:{5}/{6}?charset={7}'.format(
            self.__conSet['dialect'],
            self.__conSet['driver'],
            self.__conSet['username'],
            self.__conSet['password'],
            self.__conSet['host'],
            self.__conSet['port'],
            self.__conSet['database'],
            self.__conSet['charset']
        )
        conn = create_engine(engineInfo).connect()

        imgcollection = 'LANDSAT/LC08/C02/T1_L2'
        landsat8_bands = ['SR_B1', 'SR_B2', 'SR_B3', 'SR_B4', 'SR_B5', 'SR_B6', 'SR_B7']
        for row in eData.itertuples():
            bData = self.getPointSR_FromCollections((float(row[3]), float(row[2])),
                                                    landsat8_bands,
                                                    imgcollection,
                                                    self.__timeSet(int(row[4]), int(row[5]), int(row[6]), int(row[7])))
            bData = bData.drop(['longitude', 'latitude'], axis=1)
            bData = bData.dropna(axis=0, how='any')
            bData['DisNo.'] = row[1]
            bData.to_sql(name=self.__lat8_bandsInfo_table, con=conn, if_exists='append', index=False)

        conn.close()

    def initBandsInfo2DB(self):
        self.initLat8ToDB()


if __name__ == '__main__':
    emdat = Emdat()
    # print(emdat.getEmdatFromMysql())
    emdat.emdat_spider()
    # emdat.initBandsInfo2DB()
    # COPERNICUS/S2_SR_HARMONIZED
    # s2_bands = ['B1','B2','B3','B4','B5','B6','B7','B8','B8A','B9','B11','B12']
    # landsat8_bands = ['SR_B1', 'SR_B2', 'SR_B3', 'SR_B4', 'SR_B5', 'SR_B6', 'SR_B7']
    # re = emdat.getPointSR_FromCollections((7.305, 100.061), s2_bands, "COPERNICUS/S2_SR_HARMONIZED", ["2020-01-15", "2020-01-30"], scale=90)
    # re.to_csv("test.csv")




