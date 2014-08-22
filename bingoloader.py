import csv
import json
import os
import re
import traceback
import urllib.request
from collections import Iterable
from datetime import datetime, timedelta   
from multiprocessing import Pool

SRL_API_URL = "http://api.speedrunslive.com/"
RACES_URL = SRL_API_URL + "/pastraces?game=oot"
BOARD_API_URL = "http://giuocob.herokuapp.com/api/bingo/card"

def loadJsonFromUrl(url):
    jsonFile = urllib.request.urlopen(url)
    jsonDict = json.loads(jsonFile.read().decode())
    return jsonDict            

def getBingoBoardJson(seed):
    boardUrl = BOARD_API_URL + "?seed=" + str(seed)
    return loadJsonFromUrl(boardUrl)

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

IS_BINGO_REGEX = re.compile(".*speedrunslive.com/tools/oot-bingo/\?.*seed=[0-9]+")
BINGO_SEED_REGEX = re.compile("\?.*seed=([0-9]+)")

def isBingoGoal(goal):
    goal = goal.lower()
    return IS_BINGO_REGEX.match(goal) and "short" not in goal and "blackout" not in goal

def getBingoSeed(goal):
    result = BINGO_SEED_REGEX.search(goal)
    if result:
        return result.group(1)

class Race:
    def __init__(self, raceJson):
        self.raceid = raceJson["id"]
        self.date = datetime.fromtimestamp(float(raceJson["date"]))
        self.goal = raceJson["goal"]
        self.seed = getBingoSeed(self.goal)
        self.board = Board(getBingoBoardJson(self.seed))
        self.results = [Result(resultJson) for resultJson in raceJson["results"]]

    def writeToCsv(self, csv):
        csv.writerow(["race id", self.raceid])
        csv.writerow(["bingo seed", self.board.seed])
        csv.writerow(["date: ", self.date])
        csv.writerow([]) 
        csv.writerow(["goals"])
        for row in range(len(self.board.goals) // 5):
            csv.writerow(self.board.goals[row*5:row*5+5])
        csv.writerow([])
        csv.writerow(["results"])
        for rank, res in enumerate(self.results):
            csv.writerow([rank + 1, res.player, res.time, res.elo, res.row, res.message])

ROW_REGEX = re.compile(".*(r|c|row|col|column)\s*([1-5])", re.IGNORECASE)
BLTR_REGEX = re.compile(".*bl(-|\s)?tr", re.IGNORECASE)
TLBR_REGEX = re.compile(".*tl(-|\s)?br", re.IGNORECASE)

class Result:
    def __init__(self, resultJson):
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


class Board:
    def __init__(self, boardJson):
        self.seed = int(boardJson["seed"])
        self.version = boardJson["version"]
        self.goals = boardJson["goals"]

# concurrency settings for loads
# you can change this to tune loading performance
NUM_THREADS = 16

# other settings
# the first "race index" to load. increase this to load fewer old races
DEFAULT_START = 9000
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

            print("loaded " + str(numLoaded) + " races, including " + str(numBingos) + " bingos")

            # load all of the new race data
            # uses multiple threads to speed up loading all of the bingo boards
            print("parsing race data using " + str(NUM_THREADS) + " threads")
            threadPool = Pool(NUM_THREADS)
            races = threadPool.map(Race, bingoJsons)

            # append new race stuff to the spreadsheet
            print("writing race data to " + OUT_FILE)
            with open(OUT_FILE, "a") as outfile:
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

