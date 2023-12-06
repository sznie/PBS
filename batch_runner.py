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

smallMapsToNumAgents = {
    "small_connector": (1, 6),
    "small_corners": (1, 5),
    "small_loopchain": (1, 7),
    "small_string": (1, 6),
    "small_tree": (1, 3),
    "small_tunnel": (1, 4),
}



class BatchRunnerWindowed:
    """Class for running a single scen file"""

    def __init__(self, solver, sipp, cutoffTime, mapName, output) -> None:
        self.solver = solver
        self.sipp = sipp
        self.cutoffTime = cutoffTime
        self.map = mapName
        self.smallmap = self.map.startswith('small')
        self.output = output
        
        self.outputCSVFile = output + ".csv"

    def runSingleSettingsOnMap(self, numAgents, window, aScen, aSeed):
        # Main command
        command = "./build_debug/pbs"

        # Batch experiment settings
        command += " --seed={}".format(aSeed)
        command += " --window={}".format(window)
        command += " --agentNum={}".format(numAgents)
        
        if self.smallmap:
            command += " --map=./maps/small_maps/" + self.map + ".map"
            command += " --agents=./maps/small_scens/" + f"{self.map}-random-{aScen}.scen"
        else:
            command += " --map=./maps/" + self.map + ".map"
            command += " --agents=./maps/" + f"{self.map}.map-scen-random/{self.map}-random-{aScen}.scen"
        command += " --output={}".format(self.outputCSVFile)

        # Exp Settings
        command += " --sipp={}".format(self.sipp)
        command += " --cutoffTime={}".format(self.cutoffTime)

        print(command)

        # True if want failure error
        subprocess.run(command.split(" "), check=False)

    def detectExistingStatus(self, numAgents, window):
        if exists(self.outputCSVFile):
            df = pd.read_csv(self.outputCSVFile)
            if self.sipp:
                solverName = "SIPP"
            else:
                solverName = "PBS with AStar"
            # pdb.set_trace()
            df = df[(df["solver name"] == solverName) & (df["num agents"] == numAgents) & (df["priority window"] == window)]
            
            numFailed = len(df[(df["solution cost"] <= 0) |
                            (df["solution cost"] >= 1073741823)])
            return len(df), numFailed
        return 0, 0

    def runBatchExps(self, agentNumbers, priorityWindows, scens, seeds):
        for window in priorityWindows:
            for aNum in agentNumbers:
                numRan, numFailed = self.detectExistingStatus(aNum, window)
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
                        for aSeed in seeds:
                            print(f"    Windowed: agents={aNum}, window={window}, seed={aSeed} aScen={aScen}, map={self.map}")
                            self.runSingleSettingsOnMap(aNum, window, aScen, aSeed)
                            # stop if with this many agents more than half failed
                            numRan, numFailed = self.detectExistingStatus(aNum, window)
                            if numFailed > len(scens) / 2:
                                break
                    # Check if new run failed
                    numRan, numFailed = self.detectExistingStatus(aNum, window)
                    print(f"failed={numFailed}/{numRan}")
                    if numRan - numFailed <= len(scens) / 2:
                        print(
                            "Terminating early because all failed with {} number of agents".format(aNum))
                        break


class BatchRunnerOriginal:
    """Class for running a single scen file"""

    def __init__(self, solver, sipp, cutoffTime, mapName, output) -> None:
        self.solver = solver
        self.sipp = sipp
        self.cutoffTime = cutoffTime
        self.map = mapName
        self.smallmap = self.map.startswith('small')
        self.output = output
        
        self.outputCSVFile = output + ".csv"

    def runSingleSettingsOnMap(self, numAgents, aScen, aSeed):
        # Main command
        command = "./og_pbs/PBS/pbs"

        # Batch experiment settings
        command += " --seed={}".format(aSeed)
        command += " --agentNum={}".format(numAgents)
        if self.smallmap:
            command += " --map=./maps/small_maps/" + self.map + ".map"
            command += " --agents=./maps/small_scens/" + f"{self.map}-random-{aScen}.scen"
        else:   
            command += " --map=./maps/" + self.map + ".map"
            command += " --agents=./maps/" + f"{self.map}.map-scen-random/{self.map}-random-{aScen}.scen"
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
            df = df[(df["solver name"] == solverName) & (df["num agents"] == numAgents)]
            
            numFailed = len(df[(df["solution cost"] <= 0) |
                            (df["solution cost"] >= 1073741823)])
            return len(df), numFailed
        return 0, 0

    def runBatchExps(self, agentNumbers, scens, seeds):
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
                    for aSeed in seeds:
                        print(f"    Original: agents={aNum}, aScen={aScen}, aSeed={aSeed} map={self.map}")
                        self.runSingleSettingsOnMap(aNum, aScen, aSeed)
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


def pbsExps(mapName, run=["window", "original"]):
    if mapName in mapsToNumAgents:
        lo, hi = mapsToNumAgents[mapName]
        lo = 100
        agentRange = range(lo, hi+1, 100)
        scens = list(range(1,26))
        windows = [1, 4, 16, 64]
    
    elif mapName in smallMapsToNumAgents:
        scens = list(range(1,2))
        lo, hi = smallMapsToNumAgents[mapName]
        agentRange = range(lo, hi+1, 1)
        windows = [1, 2, 3, 4, 5]

    seeds = list(range(0, 5))
    
    if "original" in run:
        LOGPATH = "logs"
        batchFolderName = os.path.join(LOGPATH, "PBS/")

        expSettings = dict(
            solver="SpaceTimeAStar",
            cutoffTime=60,
            mapName=mapName,
            output=batchFolderName + mapName,
            sipp=False,
        )

        myBR = BatchRunnerOriginal(**expSettings)
        myBR.runBatchExps(agentRange, scens, seeds)

    if "window" in run:
        LOGPATH = "logs"
        batchFolderName = os.path.join(LOGPATH, "windowedPBS/")

        expSettings = dict(
            solver="SpaceTimeAStar",
            cutoffTime=60,
            mapName=mapName,
            output=batchFolderName + mapName,
            sipp=False,
        )
        
        myBR = BatchRunnerWindowed(**expSettings)
        myBR.runBatchExps(agentRange, windows, scens, seeds)



if __name__ == "__main__":

    # for map in ["random-32-32-20", "Paris_1_256", "den312d", "empty-48-48"]:
        # pbsExps(map, ["original", "window"])
    for map in ["small_connector", "small_corners", "small_loopchain", "small_string", "small_tree", "small_tunnel"]:
        pbsExps(map, ["window"])