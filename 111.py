import geemap
import ee
import os
import pandas as pd
import time
import pymysql

import configparser

# configparser init
cfn = configparser.ConfigParser()
ee.Initialize()


# p1 = ee.Geometry.Point([100.061, 7.305], 'EPSG: 4326')
# p2 = ee.Geometry.Point([8.305, 101.061])
#
# roi = ee.FeatureCollection(ee.List([ee.Feature(p1).set('name', 'p1'),
#                                     ee.Feature(p2).set('name', 'p2')]))
#
# dataset = ee.ImageCollection('LANDSAT/LC08/C02/T1_L2')\
#     .filterDate('2017-01-30', '2017-9-20')

# for im in dataset:
#     print(im.scale)
# out_dir = 'data/'
# out_csv = os.path.join(out_dir, 'lant8Test.csv')
# def test1(image):
#     re = geemap.extract_values_to_points(roi, image)
#     return re
# k = dataset.map(test1)

# geom_values = dataset.select(['SR_B1', 'SR_B2', 'SR_B3', 'SR_B4', 'SR_B5', 'SR_B6', 'SR_B7']).getRegion(geometry=p1, scale=30)
# # print(geom_values)
#
# geom_values_list = ee.List(geom_values).getInfo()
# header = geom_values_list[0]
# geom_df = pd.DataFrame(geom_values_list[1:], columns=header)
# df_time = geom_df['time']
# geom_df['datetime'] = pd.to_datetime(geom_df['time'],unit='ms', utc=False)
# strTime = geom_df.datetime.map(lambda x: x.strftime('%Y-%m-%d'))
#
# geom_df['SR_B1'] = geom_df['SR_B1'].map(lambda x:x * 2.75e-05 - 0.2)
# print(geom_df.filter(items=['SR_B1', 'SR_B2', 'SR_B3', 'SR_B4', 'SR_B5', 'SR_B6', 'SR_B7']))

# cali_geom_values = dataset.map(calibration).getRegion(geometry=p1, scale=30)
# print(cali_geom_values)

def maskL8sr(image):
    qaMask = image.select('QA_PIXEL').bitwiseAnd(37).eq(0)
    saturationMask = image.select('QA_RADSAT').eq(0)

    # opticalBands = image.select('SR_B.').multiply(0.0000275).add(-0.2)
    # thermalBand = image.select('ST_B.*').multiply(0.00341802).add(149.0)

    return image.updateMask(qaMask)\
        .updateMask(saturationMask)

def calibration_imgcollection(imgcolltion, bands, scale, offset):

    def calibration(image):
        newBand = image.select(bands).multiply(scale).add(offset)
        return image.addBands(srcImg = newBand, overwrite = True)

    cal_imgcolltion = imgcolltion.map(calibration)
    return cal_imgcolltion

def getPointSR_FromCollections(point_axis, bands, srcImgColletion, time, scale=30):

    # set imgCollection
    dataset = ee.ImageCollection(srcImgColletion)\
        .filterDate(time[0], time[1])\
        .map(maskL8sr)

    # calibration
    if srcImgColletion.split('/')[1] == 'LC08':
        dataset = calibration_imgcollection(dataset, bands, 2.75e-05, -0.2)

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

def timeSet(startYear, startMonth, startDay, endYear, endMonth, endDay):
    if startDay == None or endDay == None:
        print('noday')


def getEmdatFromMysql(conSet, headers):
    '''
    get emdat data(Points) from mysql,
    set mysql connect args from emdatGee.ini
    args:
    :param conSet: settings of mysql connection
    :type conSet: dict

    :param headers: select items
    :type headers: list

    :return:dataframe of points
    '''

    # connect mysql
    conn = pymysql.connect(
        host=conSet['host'],
        port=int(conSet['port']),
        user=conSet['user'],
        password=conSet['password'],
        db=conSet['db'],
        charset=conSet['charset']
    )

    # headers to sql
    select_items = '`' + str(headers[0]) + '`'
    for i in headers[1:]:
        select_items = select_items + ', `' + i + '`'

    # set select sql
    cur = conn.cursor()
    cur.execute("SELECT" + select_items +
                " FROM `em-dat data_2017-2023nature` em "
                "WHERE em.Latitude != 'Null' AND em.Longitude != 'Null'")

    re = cur.fetchall()
    return pd.DataFrame(re,columns=headers)

if __name__ == '__main__':

    print('666666')

    # cfn.read('emdatGee.ini')
    # conSet = dict(cfn.items('mysql_connset'))
    # emdat_headers = eval(dict(cfn.items('mysql_attribute'))['emdat_headers'])
    # print(emdat_headers)
    # satellite_bands = dict(cfn.items('satellite_bands'))
    # landsat8_bands = eval(satellite_bands['landsat8_bands'])
    # sentinal2_bands = eval(satellite_bands['sentinal2_bands'])
    #
    # # get emdat data test
    # # # set selecting headers
    # headers = ['DisNo.', 'Latitude', 'Longitude',
    #            'Start Year', 'Start Month', 'Start Day',
    #            'End Year', 'End Month', 'End Day']
    #
    # # data = getEmdatFromMysql(conSet, emdat_headers)
    # # print(data.loc[0])
    #
    # # get LS8 band data test
    # point = [100.061, 7.305]
    # # landsat8_bands=['SR_B1', 'SR_B2', 'SR_B3', 'SR_B4', 'SR_B5', 'SR_B6', 'SR_B7']
    # imgcollection = 'LANDSAT/LC08/C02/T1_L2'
    # time = ['2017-01-01', '2017-12-31']
    #
    #
    # df = getPointSR_FromCollections(point, landsat8_bands, imgcollection, time)
    # print(df.info())
    # print(df)
    # df.to_csv('data/test.csv')
