from typing import List, Dict
from decimal import Decimal
from datetime import datetime

"""Green:---
VendorID,
lpep_pickup_datetime,
lpep_dropoff_datetime,
store_and_fwd_flag,
RatecodeID,
PULocationID,
DOLocationID,
passenger_count,
trip_distance,
fare_amount,extra,
mta_tax,
tip_amount,
tolls_amount,
ehail_fee,
improvement_surcharge,
total_amount,
payment_type,
trip_type,
congestion_surcharge"""



class Ride:
    def __init__(self, arr: List[str]):
        #self.vendor_id = arr[0]
        self.pickup_datetime = datetime.strptime(arr[1], "%Y-%m-%d %H:%M:%S"),
        self.dropOff_datetime = datetime.strptime(arr[2], "%Y-%m-%d %H:%M:%S"),
        #self.store_and_fwd_flag = arr[3]
        #self.RatecodeID = int(arr[4])
        self.PULocationID = int(arr[5])
        self.DOLocationID = int(arr[6])
        #self.passenger_count = int(arr[7])
        #self.trip_distance = arr[8]
        #self.fare_amount = arr[9]
        #self.extra = Decimal(arr[10])
        #self.mta_tax = Decimal(arr[11])
        #self.tip_amount = Decimal(arr[12])
        #self.ehail_fee = Decimal(arr[13])
        #self.improvement_surcharge = arr[14]
        #self.total_amount = Decimal(arr[15])
        #self.trip_type= arr[16]
        #self.congestion_surcharge = Decimal(arr[17])
        self.texi_type="GREEN"

    @classmethod
    def from_dict(cls, d: Dict):
        return cls(arr=[
            #d['vendor_id'],
            d['tpep_pickup_datetime'][0],
            d['tpep_dropoff_datetime'][0],
            #d['passenger_count'],
            #d['trip_distance'],
            #d['rate_code_id'],
            #d['store_and_fwd_flag'],
            d['pu_location_id'],
            d['do_location_id'],
            #d['payment_type'],
            #d['fare_amount'],
            #d['extra'],
            #d['mta_tax'],
            #d['tip_amount'],
            #d['tolls_amount'],
            #d['improvement_surcharge'],
            #d['total_amount'],
            #d['trip_type'],
            #d['congestion_surcharge'],
            #d["texi_type"],
        ]
        )

    def __repr__(self):
        return f'{self.__class__.__name__}: {self.__dict__}'