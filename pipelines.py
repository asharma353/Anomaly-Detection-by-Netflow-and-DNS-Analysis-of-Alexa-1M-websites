# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: http://doc.scrapy.org/en/latest/topics/item-pipeline.html

class AlexaPipeline(object):
    def process_item(self, item, spider):
        return item

import csv
 
class CsvWriterPipeline(object):
 
    def __init__(self):
        #self.csvwriter = csv.writer(open('categories.csv', 'wb'))
        self.csvwriter1 = csv.writer(open('./categories/categories_sports.csv', 'wb'))


    def process_item(self, item, alexa):
        # build your row to export, then export the row
       # row = [item['Name'][0]]
        for x in item['Name']:
            row = item['Name']

          #  row.extend(x)
        self.csvwriter1.writerow(row)
        return item
