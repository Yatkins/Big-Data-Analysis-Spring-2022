import csv
import datetime
import json
from mrjob.job import MRJob
from mrjob.step import MRStep

class MRHW2(MRJob):
    
    def mapper(self, _, row):
      line = row.split(',(?![^\(]*[\)])')
      reader = csv.reader(line)
      reader = list(enumerate(reader))

      if reader[0][1][0] == 'placekey':
        return
      else:
        visits_by_day = reader[0][1][16]
        visits_by_day = json.loads(visits_by_day)
        date = reader[0][1][12]

        if '2020-03-16' in date and visits_by_day[0] > 0 and sum(visits_by_day[1:7]) == 0:
            yield date, 1
        elif '2020-03-30' in date and visits_by_day[1] > 0 and sum(visits_by_day[2:7]) == 0:
            yield date, 1
        else:
          pass

    def reducer(self, day, count):
      if '2020-03-16' in day: 
        yield f"The number of restaurants in NYC closed from March 17, 2020", sum(count)
      if '2020-03-30' in day: 
        yield f"The number of restaurants in NYC closed from April 01, 2020", sum(count)

if __name__ == '__main__':
  MRHW2.run()