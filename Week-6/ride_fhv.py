from typing import List, Dict
from decimal import Decimal
from datetime import datetime

# Index(['dispatching_base_num', 'pickup_datetime', 'dropOff_datetime',
#        'PUlocationID', 'DOlocationID', 'SR_Flag', 'Affiliated_base_number']


class Ride:
    def __init__(self, arr: List[str]):
        #self.dispatching_base_num = arr[1]
        self.pickup_datetime = datetime.strptime(arr[2], "%Y-%m-%d %H:%M:%S"),
        self.dropOff_datetime = datetime.strptime(arr[3], "%Y-%m-%d %H:%M:%S"),
        self.PULocationID = int(eval(arr[4]))
        self.DOLocationID = int(eval(arr[5]))
        #self.SR_Flag = arr[6]
        #self.Affiliated_base_number = arr[7]
        self.texi_type="FHV"
     

    @classmethod
    def from_dict(cls, d: Dict):
        return cls(arr=[
            #d['dispatching_base_num'],
            d['pickup_datetime'][0],
            d['dropOff_datetime'][0],
            d['PULocationID'],
            d['DOLocationID'],
            #d['SR_Flag'],
            #d['Affiliated_base_number'],
            d["texi_type"],
        ]
        )

    def __repr__(self):
        return f'{self.__class__.__name__}: {self.__dict__}'