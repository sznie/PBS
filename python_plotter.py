import matplotlib # RVMod
matplotlib.use('Agg') # RVMod
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

import os
import re # For regex
import pdb # For debugging

def loadAndCleanDf(filePath, discardFailures=True):
    def createName(row):
        return "PBSw{}".format(row["priority window"])

    df = pd.read_csv(filePath)
    df['name'] = df.astype('O').apply(lambda row: createName(row), axis=1) # Name settings
    df.drop(["solver name"], axis=1, inplace=True)

    #### Aggregate over seeds
    joinColumn = ["name", "instance name", "num agents"]
    resultCols = [x for x in df.columns if x not in joinColumn]
    def aggStats(data):
        d = {}
        d["success"] = np.mean((data["solution cost"] < 1073741823) & (data["solution cost"] > 0))
        if discardFailures:
            data = data[(data["solution cost"] < 1073741823) & (data["solution cost"] > 0)] # Remove failures
        for aKey in resultCols:
            d[aKey] = data[aKey].median()
        return pd.Series(d)
    
    dfMean = df.groupby(joinColumn, as_index=False, sort=False).apply(aggStats)
    return dfMean

def addBaseline(df, baseDf, joinColumns):
    def mergeDfs(leftDfSuffix, leftDf, rightDfSuffix, rightDf):
        df = pd.merge(leftDf, rightDf, how="inner", left_on=joinColumns, right_on=joinColumns, 
                                                    suffixes=[leftDfSuffix, rightDfSuffix])
        return df
    # pdb.set_trace()
    tmpDf = mergeDfs("", df, "_baseline", baseDf)
    # df = pd.concat([df, tmpDf], ignore_index=True, sort=False)
    # return df
    return tmpDf

def plotInitialResults(dfPath, ind):
    end_name = dfPath.split("/")[-1].split(".")[0] # Removes .csv
    saveFolder = "/".join(dfPath.split("/")[:-1])

    yKey = ["solution cost", "success"][ind]
    yLabel = ["Path_Cost", "Success"][ind]
    if yKey != "success":
        df = loadAndCleanDf(dfPath)
        df = df[df["success"] >= 0.5]
    else:
        df = loadAndCleanDf(dfPath, discardFailures=False)

    baselineDf = df[df["name"] == "PBSw64"]
    df = addBaseline(df, baselineDf, ["instance name", "num agents"])
    def lsAgg(d):
        # print(d[yKey])
        if yKey == "success":
            ans = {yKey: np.mean(d[yKey])}
            ans[yKey+"_baseline"] = np.mean(d[yKey+"_baseline"])
        else:
            ans = {yKey: np.median(d[yKey])}
            ans[yKey+"_baseline"] = np.median(d[yKey+"_baseline"])
        return pd.Series(ans)

    # ### All the columns we care about in our plots
    plotColumns = ["name", "priority window", "num agents"]
    lsdf = df.groupby(plotColumns, as_index=False).apply(lsAgg)
    lsdf.reset_index(drop = True, inplace = True)
    lsdf.columns = lsdf.columns.map("".join)
    lsdf.sort_values(by=["num agents", "priority window"], inplace=True)
    # pdb.set_trace()
    ncols = 2
    nrows = 1
    fig = plt.figure(figsize=(ncols*6+4,nrows*6))
    ### Plot x-axis is num agents, y-axis is yKey, color is priority window
    ax = plt.subplot(nrows, ncols, 1)
    for label, adf in lsdf.groupby(["name"], as_index=False):
        ax.plot(adf["num agents"], adf[yKey], label=label, marker='o', alpha=0.6)
    ax.set_ylabel(yLabel)
    ax.set_xlabel("Number of Agents")
    ax.legend(loc='upper center', bbox_to_anchor=(0.5, -0.11), ncol=4)

    if yKey != "success":
        ax = plt.subplot(nrows, ncols, 2)
        for label, adf in lsdf.groupby(["name"], as_index=False):
            ax.plot(adf["num agents"], adf[yKey]/adf[yKey+"_baseline"], label=label, marker='o', alpha=0.6)
        ax.set_ylabel("Ratio of {} to PBSw64".format(yLabel))
        ax.set_xlabel("Number of Agents")
        ax.legend(loc='upper center', bbox_to_anchor=(0.5, -0.11), ncol=4)

    # ### Plot x-axis is priority window, y-axis is yKey, color is num agents
    # ax = plt.subplot(nrows, ncols, 2)
    # for label, adf in lsdf.groupby(["num agents"], as_index=False):
    #     ax.plot(adf["name"], adf[yKey], label=label, marker='o', alpha=0.6)
    #     # ax.boxplot(adf[yKey], widths=[20], positions=[label], showfliers=False, labels=[label])
    #     # ax.axhline(y=adf[yKey].iloc[0], color='r', linestyle='--')
    # ax.set_ylabel(yLabel)
    # ax.set_xlabel("Window Size")
    # ax.legend(loc='upper center', bbox_to_anchor=(0.5, -0.11), ncol=4)

    # pdb.set_trace()
    plt.savefig("{}/{}_{}.pdf".format(saveFolder, end_name, yLabel), bbox_inches='tight', dpi=600)
    plt.close('all')

    
if __name__ == "__main__":
    dfMain = "/home/rishi/Desktop/CMU/Research/PBS/logs/improve_dfs/regWindow"
    for aMap in ["den312d", "empty-48-48", "maze-32-32-2", "Paris_1_256", "random-32-32-20", "room-32-32-4"]:
        # plotInitialResults("{}/{}.csv".format(dfMain, aMap), 0)
        plotInitialResults("{}/{}.csv".format(dfMain, aMap), 1)
    # dfPath = "/home/rishi/Desktop/CMU/Research/PBS/logs/improve_dfs/regWindow/empty-48-48.csv"
    # plotInitialResults(dfPath, 0)
    # plotInitialResults(dfPath, 1)