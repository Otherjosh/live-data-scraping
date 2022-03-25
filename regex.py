import re
import pyodbc
from datetime import date

#oldregex = r\"?(SWEEP DETECTED|SPLIT TICKET)?(?:\:\\n)?(>>)?(\d{2,4})(?: )([A-Z]+)(?: )([a-zA-Z]{3}\d+)( \d+[a-z]{2})? (\d+\.?\d+?) (Calls|Puts) (trade) \$(\d*\.\d+)(?: *)(?: *)?(?: \([A-Za-z]+.?[A-Za-z]+?\=)?(\d*\.\d+)?(?:\))?(?: *)([a-zA-Z]* ?[a-zA-Z]+?\!?)(?:\s*)([A-Z]*)?(\[.+\])?(?: *)(\d*\:\d*\:\d*\.\d*)(?: IV\=)(\d*\.\d\%) ([-+])(\d*\.\d) ([A-Z]+.) (\d*)(?: x \$)(\d+\.\d+)(?: \- \$)(\d+\.\d+)(?: x )(\d*) ([a-zA-Z]+\d?)(?: *)(?:Vol\=)?([A-Z]*)(?: *\-? *)([A-Z]* )?(?:52WeekHigh)?(?:Vol\=)?(\d*)([kmb]?)(?: Vol\=)(\d+)([kmb])?(?:. OI\=)(\d+)([kmb])?(?:. Delta\=)(\-)?(\d+\.?\d\%)(?:\, Impact\=)(\d+)([kmb])?(?:\/\$)(\d+\.?\d+)([kmb])?(?:\, Gamma\=)(\d+)(?:\, Vega\=\$)(\d+)([mkb]?)(?: +ROHIT +)(?:[A-Z]+\=)(\d+\.\d\d)" 
oldregexPremium = r"(\d+) ([A-Z]+) ([a-zA-Z]+\d+)(?: \d+[a-z]{2})? (\d+) ([A-Za-z]+)(?: trade \$)(\d+\.\d+)(?: \([A-Za-z]+.?[A-Za-z]+?\=)?(\d+\.\d+)?(?:\)?  )([a-zA-Z]+ ?\w+?\!?)  (\[.+\])?([A-Z]+)? (\d+\:\d+\:\d+\.\d+) (?:IV\=)?(\d+\.\d+\%)? ?([+-])?(\d+\.\d+ )?([A-Z]+2?) (\d+) x \$(\d+\.\d+) \- \$(\d+\.\d+) x (\d+) ([A-Z]+.) +([A-Z]+\/?[A-Z]+?\/?[A-Z]+?)?(?: ?\-? +?)?([A-Z]+\/?[A-Z]+?)?(?: +)?(\w+)?(?:  )?Vol\=(\d+)([kmb])?\, OI\=(\d+)([kmb])?\, Delta\=(\-)?(\d+\.?\d+?)\%\, Impact\=(\d+)([kmb])?\/\$(\d+\.?\d?)([kmb])?\, Gamma\=(\d+)([kmb])?\, Vega\=\$(\d+\.?\d+?)([kmb])? +(ROHIT  )?([A-Z]{2,4})\=(\d+\.\d+) Ref  Detail  \(Premium\=\$(\d+\.\d+)([kmb])?\)"

regex = r"\"?(SWEEP DETECTED|SPLIT TICKET)?(?:\:\\n)?(>>)?(\d{2,4})(?: )([A-Z]+)(?: )([a-zA-Z]{3}\d+)( \d+[a-z]{2})? (\d+\.?\d+?) (Calls|Puts) (trade) \$(\d*\.\d+)(?: *)(?: *)?(?: \([A-Za-z]+.?[A-Za-z]+?\=)?(\d*\.\d+)?(?:\))?(?: *)([a-zA-Z]* ?[a-zA-Z]+?\!?)(?:\s*)([A-Z]*.+?)?(\[.+\])?(?: *)(\d*\:\d*\:\d*\.\d*)(?: IV\=)?(\d*\.\d\%)? ([-+])?(\d*\.\d)? ?([A-Z]+.) (\d*)(?: x \$)(\d+\.\d+)(?: \- \$)(\d+\.\d+)(?: x )?(\d*) ([a-zA-Z]+\d?)(?: *)(?:\w+)?(?:[A-Z]+\/)?(?:[A-Z]+\/)?(?:[A-Z]+ +)?(?: *\-? *)?(?:Pre-Earnings)?(?: Post-Earnings )? (?:Vol\=)?([A-Z]*)?(?: *\-? *)([A-Z]* +)?(?:Vol\=)?(\d*)([kmb]?)(?:, OI\=)(\d+)([kmb])?(?:. Delta\=)(\-)?(\d+\.?\d\%)(?:\, Impact\=)(\d+)([kmb])?(?:\/\$)(\d*?\.?\d+?)([kmb])?(?:\, Gamma\=)(\d+)([mkb])?(?:\, Vega\=\$)(\d+)([mkb]?)(?: +ROHIT +)(?:[A-Z]+\=)(\d+\.\d\d)(?: Ref +Detail)" 
#regex:  https://regex101.com/r/Uh0wxe/7
#https://regex101.com/r/Uh0wxe/8
regexPremium = r"(\d+) ([A-Z]+) ([a-zA-Z]+\d+)(?: \d+[a-z]{2})? (\d+\.?\d?) ([A-Za-z]+)(?: trade \$)(\d+\.\d+)(?: \([A-Za-z]+.?[A-Za-z]+?\=)?(\d+\.\d+)?(?:\)?  )([a-zA-Z]+ ?\w+?\!?)  (\[.+\])?([A-Z]+)? (\d+\:\d+\:\d+\.\d+) (?:IV\=)?(\d+\.\d+\%)? ?([+-])?(\d+\.\d+ )?([A-Z]+2?) (\d+) x \$(\d+\.\d+) \- \$(\d+\.\d+) x (\d+) ([A-Z]+.) +([A-Z]+\/?[A-Z]+?\/?[A-Z]+?)?(?: ?\-? +?)?([A-Z]+\/?[A-Z]+?)?(?: +)?(\w+)?(?:  )?Vol\=(\d+)([kmb])?\, OI\=(\d+)([kmb])?\, Delta\=(\-)?(\d+\.?\d+?)\%\, Impact\=(\d+)([kmb])?\/\$(\d+\.?\d+?)([kmb])?\, Gamma\=(\d+)([kmb])?\, Vega\=\$(\d+\.?\d*?)([kmb])? +(ROHIT  )?([A-Z]{2,4})\=(\d+\.\d+) Ref  Detail  \(Premium\=\$(\d+\.?\d*?)([kmb])?\)"
regexPremiumOpener = r"(Option Alert \()(\d\d\:\d\d\:\d\d AM|PM)(\)\:)(\s)(?:Top Trades and Sweeps by Premium \(\/premium [A-Z]+\)\s)([A-Z]+)(?: )(\d+\.\d+)(?: )(\+|\-)(\d+\.\d+)(?: \()(\+|\-)(\d+\.\d+)(\%\)\s)(\@CYC1)?"
regexPremiumBackdate = r"\(\/premium [A-Z]+ (\d+\/\d+)\)"

def dataCleaner(groupDict):
    #fix MKB for gamma group 38 is the MKB
    
    #this handles combining the contract when it has an end date
    #this multiplies the values that are logged as ##k, ##m, or ##b
    #returns the dict after the changes are made   
    #can change this to "if k in value instead of splitting in the regex, idk if it would be worth changing at this point tho 
    print("Top of dataCleaner, groupDict length is: " + str(len(groupDict)))
    print("current dict in dataCleaner: ")
    print(groupDict)
    if groupDict[6]:
        print(groupDict[6])
        groupDict[5] = groupDict[5] + groupDict[6]
    else:
        pass
    if groupDict[28] == "k":
        groupDict[27] = float(groupDict[27]) * 1000
    elif groupDict[28] == "m":
        groupDict[27] = float(groupDict[27]) * 1000000
    elif groupDict[28] == "b":
        groupDict[27] = float(groupDict[27]) * 1000000000
    else:
        pass
    if groupDict[30] == "k":
        groupDict[29] = float(groupDict[29]) * 1000
    elif groupDict[30] == "m":
        groupDict[29] = float(groupDict[29]) * 1000000
    elif groupDict[30] == "b":
        groupDict[29] = float(groupDict[29]) * 1000000000
    else:
        pass
    
    if groupDict[36] == "k":
        groupDict[35] = float(groupDict[35]) * 1000
    elif groupDict[36] == "m":
        groupDict[35] = float(groupDict[35]) * 1000000
    elif groupDict[36] == "b":
        groupDict[35] = float(groupDict[35]) * 1000000000
    else:
        pass
    if groupDict[37] == "k":
        groupDict[36] = float(groupDict[36]) * 1000
    elif groupDict[37] == "m":
        groupDict[36] = float(groupDict[36]) * 1000000
    elif groupDict[37] == "b":
        groupDict[36] = float(groupDict[36]) * 1000000000

    if groupDict[39] == "k":
        groupDict[38] = float(groupDict[38]) * 1000
    elif groupDict[39] == "m":
        groupDict[38] = float(groupDict[38]) * 1000000
    elif groupDict[39] == "b":
        groupDict[38] = float(groupDict[38]) * 1000000000

    return groupDict

def assembler(groupDict):
    #inserted group 38 to handle MKB for gamma, should need to +1 the entries after 38 i think? consider before changing!
    #i changed without considering, hope it works
    print("Top of assembler, groupDict length is: " + str(len(groupDict)))
    print("current dict in assembler: ")
    print(groupDict)
    columns = ["Qty", "Ticker", "Contract", "Strike", "Side", "Trade", "Theo Price", "Note",
    "S/M", "Time", "Ecn", "IV", "IV Change", "Bid", "Ask", "Ecn1", "Vol", "Ol",
    "Delta", "Impact", "Impact($)", "Gamma", "Vega", "Underlying Price", "premium", "asofdate"]
    asofdate = str(date.today())
    dateList = asofdate.split('-')
    asofdate = f"{dateList[1]}/{dateList[2]}"
    values = [groupDict[3], groupDict[4], groupDict[5], groupDict[7],
                groupDict[8], groupDict[10], groupDict[11], groupDict[12], groupDict[13],
                groupDict[15], groupDict[19], groupDict[16], (groupDict[17] + groupDict[18]), groupDict[21],
                groupDict[22], groupDict[24], groupDict[27], groupDict[29], groupDict[32],
                groupDict[33], groupDict[35], groupDict[37], groupDict[39],
                groupDict[41], None, asofdate]  #13 might need to be 14  

    return values

def premiumAssembler(groupDict):
    try:
        IVchange = groupDict[13]+groupDict[14]
        values = [groupDict[1], groupDict[2], groupDict[3], groupDict[4], groupDict[5], groupDict[6], 
                  groupDict[7], groupDict[8], groupDict[9], groupDict[11], groupDict[10], groupDict[12], 
                  IVchange, groupDict[17], groupDict[18], groupDict[20], groupDict[24],
                  groupDict[26], groupDict[29]+'%', groupDict[30], groupDict[32],
                  groupDict[34], groupDict[36], groupDict[40], groupDict[41], groupDict['asofdate']]
    except: 
        values = [groupDict[1], groupDict[2], groupDict[3], groupDict[4], groupDict[5], groupDict[6], 
                  groupDict[7], groupDict[8], groupDict[9], groupDict[11], groupDict[10], groupDict[12], 
                  "N/A", groupDict[17], groupDict[18], groupDict[20], groupDict[24],
                  groupDict[26], groupDict[29]+'%', groupDict[30], groupDict[32],
                  groupDict[34], groupDict[36], groupDict[40], groupDict[41], groupDict['asofdate']]
    return values
    # except: 
    #     values = [int(groupDict[1]), groupDict[2], groupDict[3], float(groupDict[4]), groupDict[5], float(groupDict[6]), 
    #               float(groupDict[7]), groupDict[8], groupDict[9], groupDict[11], groupDict[10], groupDict[12], 
    #               "n/a", float(groupDict[17]), float(groupDict[18]), groupDict[20], float(groupDict[24]),
    #               float(groupDict[26]), groupDict[29]+'%', float(groupDict[30]), float(groupDict[32]),
    #               float(groupDict[34]), float(groupDict[36]), float(groupDict[40]), float(groupDict[41]), groupDict['asofdate']]
    #     return values

def premiumCleaner(groupDict):
    print("current dict in premiumCleaner: ")
    print(groupDict)
    if groupDict[25] == "k":
        groupDict[24] = float(groupDict[24]) * 1000
    elif groupDict[25] == "m":
        groupDict[24] = float(groupDict[24]) * 1000000
    elif groupDict[25] == "b":
        groupDict[24] = float(groupDict[24]) * 1000000000
    else:
        pass
    if groupDict[27] == "k":
        groupDict[26] = float(groupDict[26]) * 1000
    elif groupDict[27] == "m":
        groupDict[26] = float(groupDict[26]) * 1000000
    elif groupDict[27] == "b":
        groupDict[26] = float(groupDict[26]) * 1000000000
    else:
        pass
    if groupDict[31] == "k":
        groupDict[30] = float(groupDict[30]) * 1000
    elif groupDict[31] == "m":
        groupDict[30] = float(groupDict[30]) * 1000000
    elif groupDict[31] == "b":
        groupDict[30] = float(groupDict[30]) * 1000000000
    else:
        pass
    if groupDict[33] == "k":
        groupDict[32] = float(groupDict[32]) * 1000
    elif groupDict[33] == "m":
        groupDict[32] = float(groupDict[32]) * 1000000
    elif groupDict[33] == "b":
        groupDict[32] = float(groupDict[32]) * 1000000000
    else:
        pass
    if groupDict[35] == "k":
        groupDict[34] = float(groupDict[34]) * 1000
    elif groupDict[35] == "m":
        groupDict[34] = float(groupDict[34]) * 1000000
    elif groupDict[35] == "b":
        groupDict[34] = float(groupDict[34]) * 1000000000
    else:
        pass
    if groupDict[37] == "k":
        groupDict[36] = float(groupDict[36]) * 1000
    elif groupDict[37] == "m":
        groupDict[36] = float(groupDict[36]) * 1000000
    elif groupDict[37] == "b":
        groupDict[36] = float(groupDict[36]) * 1000000000
    else:
        pass
    if groupDict[42] == "k":
        groupDict[41] = float(groupDict[41]) * 1000
    elif groupDict[42] == "m":
        groupDict[41] = float(groupDict[41]) * 1000000
    elif groupDict[42] == "b":
        groupDict[41] = float(groupDict[41]) * 1000000000
    else:
        pass
    
    return groupDict
    

def sqlInserter(values):
    values = tuple(values)
    print("############################# inserting data in sql table: ")
    print(values)
    #CHANGE THE LINE BELOW WITH NEW SQL DB INFO
    cnxn = pyodbc.connect('Driver={SQL Server Native Client 11.0};'
                      'Server=DESKTOP-6UAGT9C\SQLEXPRESS;'
                      'Database=Stocks;'
                      'Trusted_Connection=yes;')
    cursor = cnxn.cursor()
    cursor.execute("""
                   INSERT INTO staging
                   (qty, ticker, contract, Strike, side, trade, theo_price, note, sm, time, ecn, iv, iv_change, bid, ask, ecn1, vol, oi, delta, impact, impact_dollars, gamma, vega, underlying_price, premium, asofdate)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                   """, values 
                   )
    cursor.commit()


def regexLine(regex, line):
    matches = re.finditer(regex, line, re.MULTILINE)
    groupDict = {}
    for matchNum, match in enumerate(matches, start=1):

        #print ("Match {matchNum} was found at {start}-{end}: {match}".format(matchNum = matchNum, start = match.start(), end = match.end(), match = match.group()))

        for groupNum in range(0, len(match.groups())):
            groupNum = groupNum + 1
            groupDict[groupNum] = match.group(groupNum)

            #print ("Group {groupNum} found at {start}-{end}: {group}".format(groupNum = groupNum, start = match.start(groupNum), end = match.end(groupNum), group = match.group(groupNum)))
    print(groupDict)
    return groupDict
    

#this is the columns in the table in order, didn't end up needing it in a list but leaving it as it may end up being helpful 
columns = ["Qty", "Ticker", "Contract", "Strike", "Side", "Trade", "Theo Price", "Note",
"S/M", "Time", "Ecn", "IV", "IV Change", "Bid", "Ask", "Ecn1", "Vol", "Ol",
"Delta", "Impact", "Impact($)", "Gamma", "Vega", "Underlying Price", "premium"]

#this is the sql command I used to create the staging table 
sql_server_create = """
CREATE TABLE staging
(id int NOT NULL IDENTITY PRIMARY KEY,
qty int,
ticker varchar(50),
contract varchar(50),
Strike varchar(50),
side varchar(50),
trade varchar(50),
theo_price varchar(50),
note varchar(50),
sm varchar(50),
time time,
ecn varchar(50),
iv varchar(50),
iv_change varchar(50),
bid varchar(50),
ask varchar(50),
ecn1 varchar(50),
vol varchar(100),
oi varchar(100),
delta varchar(100),
impact varchar(100),
impact_dollars varchar(50),
gamma varchar(100),
vega varchar(100),
underlying_price varchar(50),
premium varchar(50), 
asofdate varchar(100)
)
"""