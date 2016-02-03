#!/usr/bin/env python
'''Sample program to show how to get history from the archiver appliance V4 service.
This currently has samples for scalars and waveforms.
'''

from pvaccess import *
import argparse
import datetime
import pytz

verbose = False

def getHistoryData(pv):
    request = PvObject({'query' : {'pv' : STRING, "from" : STRING}})
    request.set({'query' : {'pv' : pv, "from": "10 seconds ago"}})

    rpc = RpcClient('hist')
    response = rpc.invoke(request)
    histData = response.get()
    secondsPastEpoch = histData['value']['secondsPastEpoch']
    if secondsPastEpoch:
        nanos = histData['value']['nanoseconds']
        values = histData['value']['values']
        if not isinstance(values[0], dict):
            scalarValue = True
        else:
            scalarValue = False
        numberOfSamples = len(secondsPastEpoch)
        localTimeZone = pytz.timezone("America/Los_Angeles")
        fmt = '%Y-%m-%d %H:%M:%S %Z%z'
        for i in range(numberOfSamples):
            sampleTs = datetime.datetime.fromtimestamp(secondsPastEpoch[i], pytz.utc)
            localTs = sampleTs.astimezone(localTimeZone)
            timeStr = localTs.strftime(fmt)
            print "{0}.{1} --> {2}".format(timeStr, nanos[i], values[i] if scalarValue else values[i]['value'])


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-v", "--verbose", action="store_true", help="Enable verbose logging")
    parser.add_argument("pv", help="One or more PV names. If using multiple PV names, use commans to separate the names")
    args = parser.parse_args()
    verbose = args.verbose
    getHistoryData(args.pv) 


