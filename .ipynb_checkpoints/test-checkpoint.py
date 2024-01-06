import geemap
import ee

ee.Initialize()

#打印出数据信息
image = ee.Image('srtm90_v4')
print(image.getInfo())
roi = ee.Geometry.Polygon(
        [[[103.755, 33.293],
          [103.755, 33.093],
          [103.955, 33.093],
          [103.955, 33.295]]], None, False);


dataset = ee.ImageCollection('LANDSAT/LC08/C02/T1_L2').filterBounds(roi).filterDate('2017-05-30', '2017-9-20');
print(dataset)