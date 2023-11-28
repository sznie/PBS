import os
import sys
import time
from datetime import datetime  # For printing current datetime
import subprocess  # For executing c++ executable
import numpy as np
import argparse
import pdb
import pandas as pd
from os.path import exists


mapsToNumAgents = {
    "Paris_1_256": (50, 1000), # Verified #(large map)
    "random-32-32-20": (50, 409), # Verified #(small but tough)
    "random-32-32-10": (50, 461), # Verified
    "den520d": (50, 1000), # Verified
    "den312d": (50, 1000), # Verified #(medium)
    "empty-32-32": (50, 511), # Verified
    "empty-48-48": (50, 1000), # Verified #(medium)
    "ht_chantry": (50, 1000), # Verified
}



class BatchRunner:
    """Class for running a single scen file"""

    def __init__(self, solver, sipp, cutoffTime, mapName, output) -> None:
        self.solver = solver
        self.sipp = sipp
        self.cutoffTime = cutoffTime
        self.map = mapName
        self.output = output
        
        self.outputCSVFile = output + ".csv"

    def runSingleSettingsOnMap(self, numAgents, aScen):
        # Main command
        command = "./pbs_og"

        # Batch experiment settings
        # command += " --seed={}".format(aSeed)
        command += " --agentNum={}".format(numAgents)
        command += " --map=./maps/" + self.map + ".map"
        command += " --agents=./maps/" + f"{self.map}.map-scen-random/{self.map}-random-{aScen}.scen"
        # command += " --vertex=../data/mapf-3d/" + self.map + "_Nodes.csv"
        # command += " --edge=../data/mapf-3d/" + self.map + "_Edges.csv"
        # command += " --outputCSVFile={}".format(self.outputCSVFile)
        command += " --output={}".format(self.outputCSVFile)

        # Exp Settings
        command += " --sipp={}".format(self.sipp)
        command += " --cutoffTime={}".format(self.cutoffTime)

        # True if want failure error
        subprocess.run(command.split(" "), check=False)

    def detectExistingStatus(self, numAgents):
        if exists(self.outputCSVFile):
            df = pd.read_csv(self.outputCSVFile)
            if self.sipp:
                solverName = "SIPP"
            else:
                solverName = "PBS with AStar"
            # pdb.set_trace()
            print(df.columns)
            df = df[(df["solver name"] == solverName) & (df["num agents"] == numAgents)]
            
            numFailed = len(df[(df["solution cost"] <= 0) |
                            (df["solution cost"] >= 1073741823)])
            return len(df), numFailed
        return 0, 0

    def runBatchExps(self, agentNumbers, scens):
        for aNum in agentNumbers:
            numRan, numFailed = self.detectExistingStatus(aNum)
            if numRan >= len(scens) / 2 and numRan - numFailed <= len(scens) / 2:
            # if numFailed >= len(seeds)/2:  # Check if existing run all failed
                print(
                    "Terminating early because all failed with {} number of agents".format(aNum))
                break
            elif numRan >= len(scens):  # Check if ran existing run
                print("Skipping {} completely as already run!".format(aNum))
                continue
            else:
                ### Run across scens and seeds
                for aScen in scens:
                    print(f"    agents={aNum}, aScen={aScen}, map={self.map}")
                    self.runSingleSettingsOnMap(aNum, aScen)
                    # stop if with this many agents more than half failed
                    numRan, numFailed = self.detectExistingStatus(aNum)
                    if numFailed > len(scens) / 2:
                        break
                # Check if new run failed
                numRan, numFailed = self.detectExistingStatus(aNum)
                print(f"failed={numFailed}/{numRan}")
                if numRan - numFailed <= len(scens) / 2:
                    print(
                        "Terminating early because all failed with {} number of agents".format(aNum))
                    break


def runOnAgents(expSettings, agentRange, scens):
    myBR = BatchRunner(**expSettings)
    myBR.runBatchExps(agentRange, scens)


def pbsExps(mapName):
    LOGPATH = "logs"
    batchFolderName = os.path.join(LOGPATH, "PBS")

    expSettings = dict(
        solver="SpaceTimeAStar",
        cutoffTime=300,
        mapName=mapName,
        output=batchFolderName + mapName,
        sipp=False,
    )

    lo, hi = mapsToNumAgents[mapName]

    agentRange = range(lo, hi+1, 50)
    # seeds = list(range(1, 11))
    scens = list(range(1,26))
    runOnAgents(expSettings, agentRange, scens)


if __name__ == "__main__":

    # parser = argparse.ArgumentParser(
    #                 prog='3D MAPF',
    #                 description='For running experiments on different 3D MAPF maps',
    #                 epilog='Text at the bottom of help')

    # parser.add_argument('-m', '--map')

    # args = parser.parse_args()
    
    # runExps(args.map)
    # pbsExps(args.map)

    # for map in ["random-32-32-20", "Paris_1_256", "den312d",]:
    for map in ["random-32-32-20", "Paris_1_256", "den312d", "empty-48-48"]:
        pbsExps(map)