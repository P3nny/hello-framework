# Code used from https://github.com/wahlatlas/api/blob/main/census_table_via_api.ipynb

import pandas as pd
import requests
import json
from zipfile import ZipFile
import pyarrow

# credentials in hidden file
from dotenv import dotenv_values, load_dotenv

# timestamps
import datetime


# convenience function for timestamps while logging
def tStamp():
    return datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")


# Load credentials from .env file
load_dotenv()
usr, pwd = dotenv_values().values()

# e.g. relative to current working directory
myDownloads = "./docs/data/census/"

# Set base path for API calls
BASE_URL = "https://ergebnisse2011.zensus2022.de/api/rest/2020/"


def response2disk(resp, classVar1="", classKey1="", classVar2="", classKey2=""):

    filename = (
        resp.headers["Content-Disposition"]
        .split("=")[1]
        .replace('"', "")
        .replace(".zip", "")
    )

    # writing the modifications of the table into the filename
    destination = (
        myDownloads
        + filename
        + "_"
        + classVar1
        + "_"
        + classKey1
        + "_"
        + classVar2
        + "_"
        + classKey2
        + ".zip"
    )
    # beware we now have a zip-archive that contains a csv with a much shorter name (without classVar/classKey)

    # in case of asterisk notation convert to sensible filename
    with open(destination.replace("*", "x"), "wb") as f:

        f.write(resp.content)

    print(tStamp(), filename, "download complete")


def tab2download(tablename, classVar1="", classKey1="", classVar2="", classKey2=""):

    if tablename.find("S") == 4:

        regio = "GEOGM3"  # municipalities, pop >10k

    else:

        regio = "GEOGM1"  # municipalities, 11k for all of Germany

    try:

        response = requests.get(
            BASE_URL + "data/tablefile",
            params={
                "username": usr,
                "password": pwd,
                "name": tablename,
                "regionalvariable": regio,
                "regionalkey": "01*,02*,03*,13*",  # e.g. NDR Sendegebiet
                "classifyingvariable1": classVar1,
                "classifyingkey1": classKey1,
                "classifyingvariable2": classVar2,
                "classifyingkey2": classKey2,
                #'startyear': 2011,
                "format": "ffcsv",
                "quality": "on",  # include quality symbols, see below
                "language": "de",
                "job": "false",  # get the data directly
            },
            timeout=600,
        )  # large tables may take some time

        try:
            response2disk(
                response, classVar1, classKey1, classVar2, classKey2
            )  # save to disk for re-use
            myFilename = (
                myDownloads
                + tablename
                + "_de_flat_"
                + classVar1
                + "_"
                + classKey1
                + "_"
                + classVar2
                + "_"
                + classKey2
                + ".zip"
            )
            return tab2df(myFilename.replace("*", "x"))

        except:

            if response.status_code == 200:

                # here the api will tell you if your request could not be processed
                # e.g.
                # 'Code': 25, 'Content': 'Mindestens ein Parameter enthält ungültige Werte'
                # 'Code': 90, 'Content': 'Die angeforderte Tabelle ist nicht vorhanden.'

                try:
                    print(
                        tStamp()
                        + " : "
                        + tablename
                        + " : "
                        + str(response.json()["Status"])[0:80]
                    )
                except:
                    # in case response isn't json formatted
                    print(
                        tStamp() + " : " + tablename + " : " + str(response.text[0:300])
                    )

            else:
                # log if api times out or otherwise disconnects (500, 404...)
                print(
                    tStamp()
                    + " : "
                    + tablename
                    + " http code "
                    + str(response.status_code)
                )

    except requests.exceptions.Timeout:

        # log if this request has hit its own timeout limit as set above
        print(tStamp() + " : " + tablename + " timed out")


def tab2reuse(tablename, classVar1="", classKey1="", classVar2="", classKey2=""):

    # check if desired table is already available locally
    myFilename = (
        myDownloads
        + tablename
        + "_de_flat_"
        + classVar1
        + "_"
        + classKey1
        + "_"
        + classVar2
        + "_"
        + classKey2
        + ".zip"
    )
    try:
        myDF = tab2df(myFilename.replace("*", "x"))
    except:
        print("... will fetch from database, thank you for your patience ...")
        myDF = tab2download(tablename, classVar1, classKey1, classVar2, classKey2)
    return myDF


def tab2df(ffcsv):
    zip_file = ZipFile(ffcsv)
    # name of internal csv file without classVar/classKey
    zcsv = ffcsv.split(myDownloads)[1][:18] + ".csv"
    # decimal setting for german default
    df = pd.read_csv(
        zip_file.open(zcsv),
        delimiter=";",
        decimal=",",
        # quality indicators (strings) may replace numerical values
        na_values=["...", ".", "-", "/", "x"],
        # regional key (ARS, AGS) has leading zeroes, force as string
        dtype={"1_variable_attribute_code": str},
    )

    print(ffcsv, "is ready to use")
    return df


# pruned table for a specific research question
myTable = tab2reuse("5000H-2005", "HSHGR2", "PERSON01", "WHGFL3", "WFL200BXXX")

print(myTable)
