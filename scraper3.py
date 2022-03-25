import configparser
from selenium import webdriver
import time
from selenium.webdriver.common.keys import Keys
from selenium.webdriver import ActionChains
from selenium.webdriver.common.by import By
from w3lib.html import remove_tags
import re
import pyodbc
from timeloop import Timeloop
from datetime import timedelta
from datetime import date

#used these lines of data for testing 
dataLine = ">>200 SPY Jan22 14th 475 Calls trade $0.69 (CboeTheo=0.68)  ASKSIDE  [CBOE] 14:41:54.152 IV=15.5% +1.0 C2 440 x $0.68 - $0.69 x 568 C2  AUCTION  Vol=75k, OI=45k, Delta=22%, Impact=4431/$2.09m, Gamma=1076, Vega=$2142  ROHIT  SPY=470.69 Ref  Detail  "
dataLine2 = ">100, AMD, Jan22, 125, Puts, $0.67, (CboeTheo=0.66), ASKSIDE , [BOX], 13:51:18.645, IV=59.7%, -4.2, MPRL 5 x $0.65 - $0.67 x 74 C2 , AUCTION , , , Vol=46k, OI=16k, Delta=-26%, Impact=2610/$333k, Vega=$229, ROHIT , , AMD=127.61 Ref, Premium=$6700, EST. IMPACT SHARES = 2610, To Sell, Detail"
tradeAlert = "<B>{size} {root} {expiry} {strike} {put_call} {price}</B> {theo} {side+} {exch} {time} <B>IV={ivol} {ivol_chg}</B> {nbbo} {cond} {events} {hilo} Vol={volume}, OI={open_int}, Delta={delta}, Impact={share_impact}/{dollar_impact}, Vega={vega_dollar} {baskets} {label} <B>{usymbol}={spot} {detail}</B>"
tradeAlert2 = "<B>{size}, {root}, {expiry}, {strike}, {put_call}, {price}</B>, {theo}, {side+}, {exch}, {time}, <B>IV={ivol}, {ivol_chg}</B>, {nbbo}, {cond}, {events}, {hilo}, Vol={volume}, OI={open_int}, Delta={delta}, Impact={share_impact}/{dollar_impact}, Vega={vega_dollar}, {baskets}, {label}, <B>{usymbol}={spot}, Premium={premium}, <B>EST. IMPACT SHARES = {share_impact}, {hedge_direction}, {detail}</B>"


def streamlog(logData):

    file = open('streamlog.txt',"a")
    file.write(str(logData)+str('\n'))
    file.close()
    print('save - '+str(logData))


def responselog(logData):

    file = open('responselog.txt',"a")
    file.write(str(logData)+str('\n'))
    file.close()
    print('save - '+str(logData))


def setUp():
    global driver
    try:
        driver.quit()
    except:
        pass
    #using firefox since chrome wouldn't let me click one of the login flow buttons 
    driver = webdriver.Firefox()
    driver.get('https://www.trade-alert.com')


def login():
    selfDirectedCustomerLogin = driver.find_element_by_xpath(
        '//div[@id="client-type-banner"]//div[@id="self-directed-customer"]/a[@class="button radius success right"]')
    driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")  #this scrolls to bottom to see the login button
    time.sleep(2)
    selfDirectedCustomerLogin.click()
    time.sleep(3)
    premiumOption = driver.find_element_by_xpath('//*[@id="login-choice2"]/li[1]/a')
    premiumOption.click()
    time.sleep(3)
    usernameField = driver.find_element_by_xpath('//*[@id="username"]')
    username = "redacted"
    usernameField.send_keys(username)
    time.sleep(3)
    passwordField = driver.find_element_by_xpath('//*[@id="password"]')
    password = "redacted"
    passwordField.send_keys(password)
    time.sleep(3)
    loginButton = driver.find_element_by_xpath('//div[@class="small-12 medium-4 columns"]/input[@value="Login"]')
    loginButton.click()


def getIncoming():
    parsedcount = 0  #thought this might be incorrectly getting set back to 0 but doesnt seem to be. hmm
    noUpdateCount = 0
    #oldUpdateCount = 0
    while True: 
        incomingList = driver.find_elements(By.XPATH, '//div[@class="incoming"]')
        print("len of incoming list: " + str(len(incomingList)))
        print(f"parsed count: {parsedcount}")
        print("////////////////////////////////////////////////////////////////////////////")
        if len(incomingList) > parsedcount:  
            noUpdateCount = 0
            incomingDiff = 0 - (len(incomingList) - parsedcount)  #negative of how many un-parsed datalines there are 
            if parsedcount == 0:
                newIncoming = incomingList
            else:
                newIncoming = incomingList[incomingDiff:]  #list of the unparsed datalines 
            for incoming in newIncoming:
                text = incoming.text  
                # print(f"text: {text}")
                # print("...........................")
                if text.count('>>') > 1:  #this will be responses from /premium 
                    opener = text.split('>>')[0]
                    #print(f"opener: {opener}")
                    tradeLabelPrem = opener.split(' (')[0]
                    #print(f"tradeLabelPrem for premium line: {tradeLabelPrem}")
                    dataLinesPrem = text.split('>>')[1:]  #this is all the datalines from the premium command, as a list
                    #print("dataLinesPrem: ")
                    #print(dataLinesPrem)
                    for dataLine in dataLinesPrem:  #iterate over each line 
                        # print("in for dataline in datalinesprem")
                        # print(dataLine)
                        # print(len(dataLine.split(',')))
                        if len(dataLine.split(',')) == 28:
                            #parseText(dataLine, tradeLabelPrem)
                            currentLine(dataLine, tradeLabelPrem).valuesMaker()
                            #parsedcount += 1  #commented this out 2/10 to try and address dupe issue, looks like it would increase parsedcount multiple times for one new incoming 
                        else:
                            print(f"len of dataLine for prem, split on comma: {str(len(dataLine.split(',')))}, parsetext failed ")
                            print(dataLine)
                            print("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~")
                            continue  #could log this
                elif text.count('>>') == 1:  #this will be streamed data 

                    tradeLabelRaw = text.split('>>')[0]
                    tradeLabelRaw = tradeLabelRaw.replace(':', '')
                    #print(f"trade label raw: {tradeLabelRaw}")
                    text = text.split('>>')[1:]
                    #print(f"txt in else @113 {text}")
                    if len(tradeLabelRaw) > 5:
                        tradeLabel = tradeLabelRaw
                        oldTradeLabel = tradeLabelRaw
                    else:
                        try:
                            tradeLabel = oldTradeLabel
                        except: 
                            tradeLabel = ''
                            pass
                    try:
                        #parseText(text, tradeLabel)
                        currentLine(text, tradeLabel).valuesMaker()
                    except ValueError:
                        print("value error")
                        print(text)
                        print(len(text))
                        pass
            parsedcount += 1        
        elif parsedcount > len(incomingList):
            print(f"parsed count {parsedcount} is higher than len {len(incomingList)}")
            parsedcount = len(incomingList)
            print("|||||||||||||||||||||||||||||||||||||||||||||||||||||")
        elif len(incomingList) == parsedcount: 
            noUpdateCount += 1
            print("========= ", noUpdateCount, " ==========")
            #print(oldUpdateCount)
            time.sleep(3)

        if noUpdateCount > 100:
            #close the while loop if there are no updates for 100 consecutive checks
            #100 is arbitrary, mostly made to close if its still running after the fresh incoming stop when market is closed
            #each one is around 4 seconds
            break


def sqlInserter(values):  #this takes a list of values in order to insert to SQL, may change this or make a class function to do it 
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
                   (Quantity, Ticker, Expiration, Strike, PUT_CALL, Price, CBOETheoPrice, Side, Exchange, TradeTime, IV, IV_Change, NBBO_ExchangeBid, NBBO_Bid,NBBO_BidQty,NBBO_Ask,NBBO_AskQty,NBBO_ExchangeAsk, Conditions, Events, HiLo, Volume, OpenInterest, Delta, Impact_Share, Impact_Dollar, Vega, EquityBasket, Label, TickerPrice, Premium, EstImpactShares, HedgeDirection, TradeLabel, asofdate)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                   """, values 
                   )
    cursor.commit()


def enterCommands(commandList):
    #the commandList above can be added to or modified to enter different commands 
    commandField = driver.find_element_by_xpath('//*[@id="msgArea"]')
    commandEnter = driver.find_element_by_xpath('//*[@id="send-command-btn"]')
    for cmd in commandList:
        commandField.send_keys(cmd)
        time.sleep(2)
        commandEnter.click()
        time.sleep(2)


#new content below
def numberCleaner(string):
    #this function takes a string of a shortened number (ex 333k) and converts it to just being the number value, returns an int
    if 'k' in string:
        number = string.replace('k', '')
        number = float(number) * 1000
        #print(number)
        return int(number) 
    elif 'm' in string:
        number = string.replace('m', '')
        number = float(number) * 1000000
        #print(number)
        return int(number)
    elif 'b' in string:
        number = string.replace('b', '')
        number = float(number) * 1000000000
        #print(number)
        return int(number)
    else:  #if none of these multipliers are present, just return the argument
        return string


#this is in progress, not currently used 
# def parseText(dataLine, tradelabel):  
#     #this should just take a line + tradelabel and be able to get it ready to insert/insert it 
#     if dataLine.count(">>") > 1:
#         #this is a premium line 
#         splitLine = dataLine.split(',')
#         opener = splitLine[0]
#         tradeLabelPrem = opener.split(' (')[0]
#         splitLine = splitLine[1:]
#         for line in splitLine:
#             #each line split by >>
#             if len(line.split(',')) == 28:
#                 currentLine(line, tradeLabelPrem).valuesMaker()
#                 parsedcount += 1
#             else:
#                 print(f"len of dataLine for prem, split on comma: {str(len(line.split(',')))}, parsetext failed ")
#                 print(line)
#                 print("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~")
#                 continue  #could log this
#     else:
#         tradeLabelRaw = text.split('>>')[0]
#         tradeLabelRaw = tradeLabelRaw.replace(':', '')
#         if len(tradeLabelRaw) > 5:
#             tradeLabel = tradeLabelRaw
#             oldTradeLabel = tradeLabelRaw
#         else:
#             try:
#                 tradeLabel = oldTradeLabel
#             except: 
#                 tradeLabel = ''
#                 pass
#         try:
#             #parseText(text, tradeLabel)
#             currentLine(text, tradeLabel).valuesMaker()
#         except ValueError:
#             print("value error")
#             print(text)
#             print(len(text))
#             continue

#     thisLine = currentLine(dataLine, tradelabel)
#     values = thisLine.valuesMaker()
#     sqlInserter(values)


class currentLine:
    def __init__(self, dataLine, tradelabel):
        # print("dataLine passed to currentLine, at object creation")
        # print(dataLine)
        if len(dataLine) == 1:
            dataLine = dataLine[0]
        try:
            dataList = dataLine.split(',')
        except:
            dataList = dataLine
            pass
        #print(f"len of dataList when creating object: {str(len(dataList))}")
        Quantity, Ticker, Expiration, Strike, PUT_CALL, Price, CBOETheoPrice, Side, Exchange, TradeTime, IV, IV_Change, NBBO, Conditions, Events, HiLo, Volume, OpenInterest, Delta, Impact, Vega, EquityBasket, Label, TickerPrice, Premium, EstImpactShares, HedgeDirection, TradeLabel = dataList
        
        #cleaning zone
        splitImpact = Impact.split('/')  #split impact into shares and dollars
        impactShareRaw = splitImpact[0]
        impactDollarRaw = splitImpact[1]
        impactShareDirty = impactShareRaw.split("=")[-1]
        impactShare = numberCleaner(impactShareDirty)
        impactDollars = impactDollarRaw.replace('$', '')
        impactDollarsClean = numberCleaner(impactDollars)  
        NBBO = NBBO.strip()
        splitNBBO = NBBO.split(' ')
        if len(splitNBBO) == 9:
            #handle if this doesn't have all the values 
            #sounds like all 0s is the only case 
            self.NBBO_ExchangeBid = splitNBBO[0].strip()  
            self.NBBO_Bid = splitNBBO[3].strip()
            self.NBBO_BidQty = splitNBBO[1].strip()
            self.NBBO_Ask = splitNBBO[5].strip()
            self.NBBO_AskQty = splitNBBO[7].strip()
            self.NBBO_ExchangeAsk = splitNBBO[8].strip()
        else:
            self.NBBO_ExchangeBid = ''
            self.NBBO_Bid = splitNBBO[2]
            self.NBBO_BidQty = splitNBBO[0]
            self.NBBO_Ask = splitNBBO[4]
            self.NBBO_AskQty = splitNBBO[6]
            self.NBBO_ExchangeAsk = ''
            
        VegaClean = Vega.split('=$')[-1]
        volumeRaw = Volume.split('=')[1]
        VolumeClean = numberCleaner(volumeRaw)
        OIprep = OpenInterest.split('=')[-1]
        OIClean = numberCleaner(OIprep)
        deltaClean = Delta.split('=')[-1]#.replace('%', '')  #optional remove %
        CBOEClean = CBOETheoPrice.split('=')[-1].replace(')', '')
        PriceClean = Price.replace('$', '')
        IVClean = IV.split('=')[-1]#.replace('%', '')  #optional remove %
        TickerPriceClean = TickerPriceClean = TickerPrice.split('=')[-1].split(' ')[0]
        PremiumRaw = Premium.split('=$')[-1]
        PremiumClean = numberCleaner(PremiumRaw)
        estImpactSharesDirty = EstImpactShares.split('= ')[-1]
        estImpactSharesClean = numberCleaner(estImpactSharesDirty)

        #self value zone
        self.Quantity = Quantity.replace('>', '')
        self.Ticker = Ticker.strip()
        self.Expiration = Expiration.strip()
        self.Strike = Strike.strip()
        self.PUT_CALL = PUT_CALL.strip()
        self.Price = PriceClean
        self.CBOETheoPrice = CBOEClean
        self.Side = Side.strip()
        self.Exchange = Exchange.strip()
        self.TradeTime = TradeTime.strip()
        self.IV = IVClean
        self.IV_Change = IV_Change.strip()
        self.NBBO = NBBO

        self.Conditions = Conditions.strip()
        self.Events = Events.strip()
        self.HiLo = HiLo.strip()
        self.Volume = VolumeClean
        self.OpenInterest = OIClean
        self.Delta = deltaClean
        self.Impact = Impact
        self.Impact_Share = impactShare  
        self.Impact_Dollar = impactDollarsClean
        self.Vega = VegaClean
        self.EquityBasket = EquityBasket.strip()
        self.Label = Label.strip()
        self.TickerPrice = TickerPriceClean
        self.Premium = PremiumClean
        self.EstImpactShares = estImpactSharesClean
        self.HedgeDirection = HedgeDirection.strip()
        self.TradeLabel = tradelabel.strip()    
        #tradelabel is passed in since not every line has it defined
        self.asofdate = date.today()
        self.oldTradeLabel = ''
        
    def add_TradeLabel(self, tradeLabel):
        self.oldTradeLabel = tradeLabel
        if len(self.TradeLabel) < 5:
            print(f"updating trade label from {self.TradeLabel} to {tradeLabel}")
            self.TradeLabel = tradeLabel

    def valuesMaker(self):
        #this creates the list of values to be passed to sqlInserter
        #dataList = [Quantity, Ticker, Expiration, Strike, PUT_CALL, Price, CBOETheoPrice, Side, Exchange, TradeTime, IV, IV_Change, NBBO, 
        # Conditions, Events, HiLo, Volume, OpenInterest, Delta, Impact, Vega, EquityBasket, Label, TickerPrice, Premium, EstImpactShares, 
        # HedgeDirection, TradeLabel, asofdate]

        values = [self.Quantity, self.Ticker, self.Expiration, self.Strike, self.PUT_CALL, self.Price,
                    self.CBOETheoPrice, self.Side, self.Exchange, self.TradeTime, self.IV, self.IV_Change, 
                    self.NBBO_ExchangeBid, self.NBBO_Bid, self.NBBO_BidQty, self.NBBO_Ask, self.NBBO_AskQty, self.NBBO_ExchangeAsk, 
                    self.Conditions, self.Events, self.HiLo, self.Volume, self.OpenInterest, self.Delta, self.Impact_Share, self.Impact_Dollar, 
                    self.Vega, self.EquityBasket, self.Label, self.TickerPrice, self.Premium, self.EstImpactShares, self.HedgeDirection, self.TradeLabel, self.asofdate]
        print(values)

        sqlInserter(values)
        #return values       

         


if __name__ == "__main__":
    config = configparser.ConfigParser()
    config.read('Config.ini')
    commandListRaw = config['Commands']['list']
    commandList = commandListRaw.split(',')
    setUp()
    login()
    time.sleep(3)
    commandList2 = ["/premium spy","+","/premium spy -1 ","+","/premium spy -2","+","/premium spy -3","+","/premium spy -4 ","+","/premium spy -5 ","+","/premium roku","+","/premium roku -1 ","+","/premium roku -2","+","/premium roku -3","+","/premium roku -4 ","+","/premium roku -5 ","+","/premium amzn","+","/premium amzn -1 ","+","/premium amzn -2","+","/premium amzn -3","+","/premium amzn -4 ","+","/premium amzn -5 ","+","/premium uber","+","/premium uber -1 ","+","/premium uber -2","+","/premium uber -3","+","/premium uber -4 ","+","/premium uber -5 ","+",]
    commandList3 = ["/premium spy -1 ", ]
    enterCommands(commandList)
    # if __name__ == "__main__":
    #     repeatCommandList = repeatCommandList
    #     tl.start(block=True)
    getIncoming()


#sql command 
# sql = "USE [Stocks]
# GO

# /****** Object:  Table [dbo].[staging]    Script Date: 1/27/2022 12:02:12 PM ******/
# SET ANSI_NULLS ON
# GO

# SET QUOTED_IDENTIFIER ON
# GO

# CREATE TABLE [dbo].[staging](
# 	[id] [int] IDENTITY(1,1) NOT NULL,
# 	[Quantity] [int] NULL,
# 	[Ticker] [varchar](50) NULL,
# 	[Expiration] [varchar](50) NULL,
# 	[Strike] [varchar](50) NULL,
# 	[PUT_CALL] [varchar](50) NULL,
# 	[Price] [varchar](50) NULL,
# 	[CBOETheoPrice] [varchar](50) NULL,
# 	[Side] [varchar](50) NULL,
# 	[Exchange] [varchar](50) NULL,
# 	[TradeTime] [time](7) NULL,
# 	[IV] [varchar](50) NULL,
# 	[IV_Change] [varchar](50) NULL,
# 	[NBBO_ExchangeBid] [varchar](50) NULL,
# 	[NBBO_Bid] [varchar](50) NULL,
# 	[NBBO_BidQty] [varchar](50) NULL,
# 	[NBBO_Ask] [varchar](100) NULL,
# 	[NBBO_AskQty] [varchar](100) NULL,
# 	[NBBO_ExchangeAsk] [varchar](100) NULL,
# 	[Conditions] [varchar](100) NULL,
# 	[Events] [varchar](50) NULL,
# 	[HiLo] [varchar](100) NULL,
# 	[Volume] [varchar](100) NULL,
# 	[OpenInterest] [varchar](50) NULL,
# 	[Delta] [varchar](50) NULL,
# 	[Impact_Share] [varchar](100) NULL,
# 	[Impact_Dollar] [varchar](50) NULL,
# 	[Vega] [varchar](50) NULL,
# 	[EquityBasket] [varchar](50) NULL,
# 	[Label] [varchar](50) NULL,
# 	[TickerPrice] [varchar](50) NULL,
# 	[Premium] [varchar](50) NULL,
# 	[EstImpactShares] [varchar](50) NULL,
# 	[HedgeDirection] [varchar](50) NULL,
# 	[TradeLabel] [varchar](50) NULL,
# 	[asofdate] [varchar](50) NULL,
# PRIMARY KEY CLUSTERED 
# (
# 	[id] ASC
# )WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
# ) ON [PRIMARY]
# GO

# "
