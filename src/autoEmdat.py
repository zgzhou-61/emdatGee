import geemap
import ee
import time

def getLadst8Img(roi, time):
    # set imageCollection
    dataset = ee.ImageCollection('LANDSAT/LC08/C02/T1_L2') \
        .filterBounds(roi) \
        .filterDate(time[0], time[1])

    # Create a cloud-free composite with default parameters.
    composite = ee.Algorithms.Landsat.simpleComposite(dataset)

    # Create a cloud-free composite with custom parameters for
    # cloud score threshold and percentile.
    customComposite = ee.Algorithms.Landsat.simpleComposite(**{
        'collection': dataset,
        'percentile': 75,
        'cloudScoreRange': 5
    })

    return customComposite

def getImgFromGee(roi_axis, time, roi_range = 10, collection = 'landsat8', scale = 20, out_dir = 'data'):

    # check roi_axis[lat, lon]
    if roi_axis[0] < -90.0 or roi_axis[0] > 90.0 or roi_axis[1] > 180.0 or roi_axis[1] < -180:
        print("Wrong roi_axis(latitude, longitude), -90.0<=lat<=90.0, -180.0<=lon<=180.0!")
        return 


    # check time
    #


    lat = roi_axis[0]
    lon = roi_axis[1]
    # set region
    roi = ee.Geometry.Polygon(
        [[[lon - roi_range / 100, lat + roi_range / 100],
          [lon - roi_range / 100, lat - roi_range / 100],
          [lon + roi_range / 100, lat - roi_range / 100],
          [lon + roi_range / 100, lat + roi_range / 100]]], None, False)


    if collection == 'landsat8':
        filename = '{}/LAST08C2T1-{}-{}.tif'.format(out_dir, time[0], time[1])
        geemap.ee_export_image(getLadst8Img(roi, time), filename=filename,
                               scale=scale, region=roi)

def getPointBandsInfo(roi_axis, ):
    pass



if __name__ == '__main__':
    ee.Initialize()
    getImgFromGee([7.305, 100.061], ['2017-7-01', '2017-8-08'], scale=20)







