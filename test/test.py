import ee
import os
import pandas as pd
import pymysql
import configparser


class Emdat():
    def __init__(self):
        ee.Initialize()
        self.__cfg = configparser.ConfigParser()
        self.__BASE_DIR = os.path.dirname(os.path.abspath(__file__))
        self.__cfg.read(os.path.join(self.__BASE_DIR, 'emdatGee.ini'))
        self.__conSet = dict(self.__cfg.items('mysql_connset'))
        self.__mysql_attribute = dict(self.__cfg.items('mysql_attribute'))
        self.__emdatData = None

        # connect mysql
        self.conn = pymysql.connect(
            host=self.__conSet['host'],
            port=int(self.__conSet['port']),
            user=self.__conSet['user'],
            password=self.__conSet['password'],
            db=self.__conSet['db'],
            charset=self.__conSet['charset']
        )

    def getEmdatFromMysql(self):
        '''
        get emdat data(Points) from mysql, set mysql connect args from emdatGee.ini
        :return:
        '''

        emdat_table = self.__mysql_attribute['emdat_table']
        emdat_headers = eval(self.__mysql_attribute['emdat_headers'])
        sql_condition = self.__mysql_attribute['sql_condition']

        # headers to sql
        select_items = '`' + str(emdat_headers[0]) + '`'
        for i in emdat_headers[1:]:
            select_items = select_items + ', `' + i + '`'

        # set select sql
        cur = self.conn.cursor()
        cur.execute("SELECT" + select_items +
                    " FROM `" + emdat_table + "` em " + sql_condition)
        # "WHERE em.Latitude != 'Null' AND em.Longitude != 'Null'")

        result = cur.fetchall()
        cur.close()
        return pd.DataFrame(result, columns=emdat_headers)

    def timeSet(self, startYear, startMonth, endYear, endMonth):
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

        # set imgCollection
        dataset = ee.ImageCollection(srcImgColletion) \
            .filterDate(time[0], time[1]) \
            .map(apply_scale_factors) \
            .map(maskL8sr)

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

    def updateBandsInfo2DB(self):

        cur = self.conn.cursor()

        # get BandsInfoTable_Name
        emdat_bandsInfo_table = self.__mysql_attribute['emdat_bandsinfo_table']
        # create emdat_bandsInfo_table
        createTableSql = "CREATE TABLE IF NOT EXISTS " + emdat_bandsInfo_table + "(" \
                      "`DisNo.` varchar(255) ," \
                      "FOREIGN KEY (`DisNo.`) REFERENCES `em-dat data_2017-2023nature` (`DisNo.`)," \
                      "id varchar(255)," \
                      "SR_B1 DECIMAL(20)," \
                      "SR_B2 DECIMAL(20)," \
                      "SR_B3 DECIMAL(20)," \
                      "SR_B4 DECIMAL(20)," \
                      "SR_B5 DECIMAL(20)," \
                      "SR_B6 DECIMAL(20)," \
                      "SR_B7 DECIMAL(20)," \
                      "datetime DATE)"

        cur.execute(createTableSql)








if __name__ == '__main__':
    emdat = Emdat()
    emdat.updateBandsInfo2DB()



