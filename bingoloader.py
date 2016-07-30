#!/usr/bin/env python3

import csv
import json
import os
import re
import traceback
import urllib.request
from collections import Iterable, Counter
from datetime import datetime, date, timedelta
from multiprocessing import Pool

SRL_URL = "http://www.speedrunslive.com/"
SRL_API_URL = "http://api.speedrunslive.com/"
RACES_URL = SRL_API_URL + "/pastraces?game=oot"
SRL_BOARD_URL = SRL_URL + "tools/oot-bingo/"
# temporarily removed from api
#BOARD_URL = "http://giuocob.com/bingo/all-version-bingo.html"
BOARD_API_URL = "http://legacybingo.bingosync.com/api/bingo/legacy/card"

def getRaceUrl(raceId):
    return SRL_URL + "races/result/#!/" + str(raceId)

def getBingoJsonUrl(seed, version=None):
    boardUrl = BOARD_API_URL + "?seed=" + str(seed)
    if version:
        boardUrl += "&version=" + version
    return boardUrl

# temporarily removed from api
def getBingoUrl(seed, version=None):
    boardUrl = BOARD_URL + "?seed=" + str(seed)
    if version:
        boardUrl += "&version=" + version
    return boardUrl

def loadJsonFromUrl(url):
    jsonFile = urllib.request.urlopen(url)
    jsonDict = json.loads(jsonFile.read().decode())
    return jsonDict

def getBingoBoardJson(seed, version=None):
    return loadJsonFromUrl(getBingoJsonUrl(seed, version))

def getRaceJson(raceIndex):
    raceUrl = RACES_URL + "&pageSize=1&page=" + str(raceIndex)
    return loadJsonFromUrl(raceUrl)["pastraces"][0]

def getRaceCount():
    emptyUrl = RACES_URL + "&pageSize=0"
    emptyJson = loadJsonFromUrl(emptyUrl)
    return int(emptyJson["count"])

def getRaceJsonsSince(beginIndex):
    raceCountBefore = getRaceCount()
    racesToLoad = raceCountBefore - beginIndex
    racesUrl = RACES_URL + "&pageSize=" + str(racesToLoad)

    raceJsons = loadJsonFromUrl(racesUrl)["pastraces"]

    # detect if changes have happened since and retry
    if raceCountBefore != getRaceCount():
        print("detected a change while loading races, retrying")
        return getRaceJsonsSince(beginIndex)
    else:
        return raceJsons

def filterNonBingos(raceJsons):
    return [raceJson for raceJson in raceJsons if isBingoGoal(raceJson["goal"])]

IS_BINGO_REGEX = re.compile(".*speedrunslive.com/tools/oot-bingo.*")
BINGO_SEED_REGEX = re.compile("\?.*seed=([0-9]+)")

def isBingoGoal(goal):
    goal = goal.lower()
    isBingo = IS_BINGO_REGEX.match(goal)
    short = "short" in goal
    blackout = "blackout" in goal
    double = "double" in goal or "anti" in goal
    return isBingo and not short and not blackout and not double

def getBingoSeed(goal):
    result = BINGO_SEED_REGEX.search(goal)
    if result:
        return result.group(1)

# The dates at which different bingo versions were released
# You can add a new version here if you like, just follow the format
# Add more recent versions to the beginninf of the list
BINGO_VERSIONS = [
 (datetime(2016, 6, 29),  "v9.1"),
 (datetime(2016, 4, 8),  "v9.0"),
 (datetime(2016, 1, 29),  "v8.5"),
 (datetime(2014, 12, 13), "v8.4"),
 (datetime(2014, 8, 21),  "v8.3"),
 (datetime(2014, 6, 13),  "v8.2"),
 (datetime(2013, 12, 12), "v8.1"),
 (datetime(2013, 9, 11),  "v8")
]

CURRENT_VERSION = BINGO_VERSIONS[0][1]

def getBingoVersionAt(raceDate):
    for versionDate, version in BINGO_VERSIONS:
        if raceDate > versionDate:
            return version
    print("could not find explicit bingo version for date: " + str(raceDate))
    return CURRENT_VERSION

class Race:
    def __init__(self, raceJson):
        self.raceid = raceJson["id"]
        self.date = datetime.fromtimestamp(float(raceJson["date"]))
        self.goal = raceJson["goal"]
        self.seed = getBingoSeed(self.goal)
        self.version = getBingoVersionAt(self.date)
        self.board = Board(getBingoBoardJson(self.seed, self.version))
        self.results = [Result(resultJson, self.board) for resultJson in raceJson["results"]]

    @property
    def raceUrl(self):
        return getRaceUrl(self.raceid)

    @property
    def bingoUrl(self):
        if self.version == CURRENT_VERSION:
            return SRL_BOARD_URL + "?seed=" + str(self.seed)
        else:
            return "Legacy Bingo urls have been temporarily removed from the api."
        return getBingoUrl(self.seed, self.version)

    def writeToCsv(self, csv):
        csv.writerow(["race id: ", self.raceid, self.raceUrl])
        csv.writerow(["bingo seed: ", self.board.seed, self.bingoUrl])
        csv.writerow(["bingo version: ", self.board.version])
        csv.writerow(["date: ", self.date])
        csv.writerow(["goal: ", self.goal])
        csv.writerow([])
        csv.writerow(["goals"])
        for goalsRow in self.board.goalsGrid:
            csv.writerow(goalsRow)
        csv.writerow([])
        csv.writerow(["results"])
        for rank, result in enumerate(self.results):
            csv.writerow([rank + 1] + result.getInfo())

ROW_REGEX = re.compile(".*(r|c|row|col|column)\s*([1-5])", re.IGNORECASE)
BLTR_REGEX = re.compile(".*bl(-|\s)?tr", re.IGNORECASE)
TLBR_REGEX = re.compile(".*tl(-|\s)?br", re.IGNORECASE)

class Result:
    def __init__(self, resultJson, board):
        self.player = resultJson["player"]
        sec = resultJson["time"]
        self.time = timedelta(seconds=sec) if sec > 0 else "forfeit"
        self.elo = resultJson["oldtrueskill"]
        self.message = resultJson["message"]
        row_result = ROW_REGEX.search(self.message)
        bltr_result = BLTR_REGEX.search(self.message)
        tlbr_result = TLBR_REGEX.search(self.message)
        # if found more than one match in the regex or no matches at all
        if sum(1 for el in [row_result, bltr_result, tlbr_result] if el) != 1:
            self.row = "---"
        elif row_result:
            row, num = row_result.groups()
            self.row = "ROW " + num if "r" in row.lower() else "COL " + num
        elif bltr_result:
            self.row = "BL-TR"
        elif tlbr_result:
            self.row = "TL-BR"
        else:
            self.row = "This should never happen"
        self.goals = board.getGoalsFromRowString(self.row)

    def getInfo(self):
        return [self.player, self.time, self.elo, self.row, self.message] + self.goals


class Board:
    def __init__(self, boardJson):
        self.seed = int(boardJson["seed"])
        self.version = boardJson["version"]
        self.goalsList = [goalJson["name"] for goalJson in boardJson["goals"]]
        self.goalsGrid = [self.goalsList[row*5:row*5+5] for row in range(5)]

    def getGoalsFromRowString(self, rowString):
        if "ROW" in rowString:
            rowIndex = int(rowString[-1]) - 1
            return self.goalsGrid[rowIndex]
        elif "COL" in rowString:
            colIndex = int(rowString[-1]) - 1
            return [self.goalsGrid[row][colIndex] for row in range(5)]
        elif rowString == "TL-BR":
            return [self.goalsGrid[index][index] for index in range(5)]
        elif rowString == "BL-TR":
            return [self.goalsGrid[4 - col][col] for col in range(5)]
        else:
            return []


# concurrency settings for loads
# you can change this to tune loading performance
NUM_THREADS = 16

# other settings
# the first "race index" to load. increase this to load fewer old races
DEFAULT_START = 12000
# this file keeps track of the last loaded race number. you can delete it to force a refresh
STORE_FILE = "lastloaded.txt"
# this file is where the program will output the data for newly loaded races
OUT_FILE = "out.csv"

# main program logic
if __name__ == "__main__":
    try:
        # figure out the last race loaded, or use the default
        if os.path.isfile(STORE_FILE):
            with open(STORE_FILE, "r") as storeFile:
                lastIndex = int(storeFile.readline())
                print("loaded last index: " + str(lastIndex))
        else:
            print("no lastloaded.txt found, using default: " + str(DEFAULT_START))
            lastIndex = DEFAULT_START

        # load all of the race data since then
        print("loading new race data")
        raceJsons = getRaceJsonsSince(lastIndex)
        numLoaded = len(raceJsons)

        # only do more processing if new bingos were loaded
        if numLoaded == 0:
            print("no new races")
        else:
            # get rid of non-bingo races
            bingoJsons = filterNonBingos(raceJsons)
            numBingos = len(bingoJsons)

            dates = [datetime.fromtimestamp(float(race["date"])) for race in bingoJsons]
            versionCounts = Counter(getBingoVersionAt(date) for date in dates)
            print("loaded " + str(numLoaded) + " races, including " + str(numBingos) + " bingos")
            print("version breakdown:")
            for version, count in sorted(versionCounts.items()):
                print(version + ":", count)

            # load all of the new race data
            if NUM_THREADS > 1:
                # uses multiple threads to speed up loading all of the bingo boards
                print("parsing race data using " + str(NUM_THREADS) + " threads")
                threadPool = Pool(NUM_THREADS)
                races = threadPool.map(Race, bingoJsons)
            else:
                # slower because single threaded
                # however, makes the stack trace easier to follow
                races = [Race(bingoJson) for bingoJson in bingoJsons]

            # append new race stuff to the spreadsheet
            print("writing race data to " + OUT_FILE)
            with open(OUT_FILE, "a", newline='', encoding='utf-8') as outfile:
                writer = csv.writer(outfile)
                for race in races:
                    race.writeToCsv(writer)
                    writer.writerow([])

            # record the last race we loaded
            lastIndex += numLoaded
            with open(STORE_FILE, "w") as storeFile:
                storeFile.write(str(lastIndex) + "\n")

    except Exception as e:
        print("oops, looks like there was an error:")
        traceback.print_exc()
        print("depending on what happened, the output files may be corrupted")

    # read an input so the window doesn't close immediately
    input("press enter to close...")

