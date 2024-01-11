import geemap
import ee
import os
import pandas as pd
import pymysql
import configparser

def maskL8sr(image):
    '''
    landsat8 cloud mask
    :param image: image of lansat8 from gee
    :return: image after cloud masking
    '''
    qaMask = image.select('QA_PIXEL').bitwiseAnd(37).eq(0)
    saturationMask = image.select('QA_RADSAT').eq(0)

    # opticalBands = image.select('SR_B.').multiply(0.0000275).add(-0.2)
    # thermalBand = image.select('ST_B.*').multiply(0.00341802).add(149.0)

    return image.updateMask(qaMask)\
        .updateMask(saturationMask)

def calibration_imgcollection(imgcolltion, bands, scale, offset):
    '''
    calibrating ImageCollection
    :param imgcolltion: the ImageCollection need to be calibrated
    :param bands: bands of the ImageCollection
    :param scale: scale of image
    :param offset: the offset of the ImageCollection
    :return: the ImageCollection after calibrating
    '''

    def calibration(image):
        newBand = image.select(bands).multiply(scale).add(offset)
        return image.addBands(srcImg = newBand, overwrite = True)

    cal_imgcolltion = imgcolltion.map(calibration)
    return cal_imgcolltion

def getPointSR_FromCollections(point_axis, bands, srcImgColletion, time, scale=30):
    '''
    get bands infomation of the taget point
    :param point_axis: axis of point
    :param bands: bands of satellite
    :param srcImgColletion: taget ImageCollection from gee
    :param time: time limit
    :param scale: scale of image
    :return: bands infomation of taget point
    '''

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

# set startSate and endDate
def timeSet(startYear, startMonth, endYear, endMonth):
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

def updateEmdatBandsInfo():
    pass

if __name__ == '__main__':
    # dateRe = timeSet(2017, 7, 2017, 12)
    # print(dateRe)

    # init
    cfg = configparser.ConfigParser()
    BASE_DIR = os.path.dirname(os.path.abspath(__file__))
    cfg.read(os.path.join(BASE_DIR, 'emdatGee.ini'))
    ee.Initialize()


    conSet = dict(cfg.items('mysql_connset'))
    print(conSet)

    # get emdat data test
    # set selecting headers
    emdat_headers = eval(dict(cfg.items('mysql_attribute'))['emdat_headers'])

    data = getEmdatFromMysql(conSet, emdat_headers)
    d0 = data.loc[0]
    print(d0)

    startYear = int(d0['Start Year'])
    startMonth = int(d0['Start Month'])
    endYear = int(d0['End Year'])
    endMonth = int(d0['End Month'])

    # get LS8 band data test
    point = [100.061, 7.305]

    # landsat8_bands=['SR_B1', 'SR_B2', 'SR_B3', 'SR_B4', 'SR_B5', 'SR_B6', 'SR_B7']
    satellite_bands = dict(cfg.items('satellite_bands'))
    landsat8_bands = eval(satellite_bands['landsat8_bands'])
    sentinal2_bands = eval(satellite_bands['sentinal2_bands'])
    imgcollection = 'LANDSAT/LC08/C02/T1_L2'
    time = timeSet(startYear, startMonth, endYear, endMonth)

    df = getPointSR_FromCollections(point, landsat8_bands, imgcollection, time)
    df = df.dropna(axis=0, how='any')
    print(df.info())
    print(df['SR_B7'])

    dataDir = os.path.abspath(os.path.join(os.getcwd(), "./"))

    print(dataDir)

    df.to_csv(os.path.join(dataDir, 'data', 'test.csv'))





