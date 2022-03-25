# -*- coding: utf-8 -*-
"""
Created on Fri Oct 22 16:21:17 2021

@author: joshu
"""

from selenium import webdriver
import time
from selenium.webdriver.common.keys import Keys
from selenium.webdriver import ActionChains
from w3lib.html import remove_tags
import re
import pyodbc
from timeloop import Timeloop
from datetime import timedelta
from datetime import date
from regex import *


global noUpdateCount, asofdate
tl = Timeloop()


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

commandList = ["/premium spy", "/premium qqq", "/premium dia", "/premium tsla",]
def enterCommands(commandList):
    #the commandList above can be added to or modified to enter different commands 
    commandField = driver.find_element_by_xpath('//*[@id="msgArea"]')
    commandEnter = driver.find_element_by_xpath('//*[@id="send-command-btn"]')
    for cmd in commandList:
        commandField.send_keys(cmd)
        time.sleep(2)
        commandEnter.click()
        time.sleep(2)


def parseText(text):
    if text.count(">>") >1: 
            #this checks if its a return from /premium and splits it if so 
            print("premium line ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~\n")
            print(text)
            text = text.split(">>")
            
            asofdate = date.today() 
            #this processes the main part of the premium return, the first part (pre->>) can be accessed with text[0] here
#####            
            backdateMatch = regexLine(regexPremiumBackdate, text[0])
            print(backdateMatch)
            if regexPremiumBackdate[1]:  #this might work? 
                try:
                    asofdate = backdateMatch[1] 
                    print(asofdate)
                except:
                    asofdate = str(date.today())
                    dateList = asofdate.split('-')
                    asofdate = f"{dateList[1]}/{dateList[2]}"
                    pass
            text = text[1:]
#####                
            for line in text:
                try:
                    print(line)
                    matches = regexLine(regexPremium, line) 
                    print(matches)
                    if matches == {}:
                        print("no matches found in line - response data")
                        responselog(line)
                        print(f"saving {line} to log")
                        continue
                    else:
                        time.sleep(3)
                        cleanedData = premiumCleaner(matches)
                        cleanedData['asofdate'] = asofdate
                        vals = premiumAssembler(cleanedData)
                        sqlInserter(vals)
                except KeyError:
                    print("key error - line 87")
    else:
##### also include asofdate in this else 
        print(text)
        matches = regexLine(regex, text)
        print(matches)
        if matches == {}:
            print("No matches found - streaming data")
            streamlog(text)
            print(f"saved {text} to log")
        else:
            time.sleep(3)
            try:
                cleanedData = dataCleaner(matches)
                print("ran dataCleaner")
                vals = assembler(cleanedData)
                print("ran assembler")
                sqlInserter(vals)
            except KeyError:
                print("key error ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~")
                streamlog(text)

def getIncoming():
    parsedcount = 0
    noUpdateCount = 0

    while True:
        #this will continually look for new incomings after completing the initial ones 
        incomingList = driver.find_elements_by_xpath('//div[@class="incoming"]')
        oldUpdateCount = 0
        print("len of incoming list: " + str(len(incomingList)))
        print(parsedcount)
        if len(incomingList) > parsedcount: #could this just be conditional on incoming diff value? 
            noUpdateCount = 0
            incomingDiff = 0 - (len(incomingList) - parsedcount)
            newIncoming = incomingList[incomingDiff:]
            for incoming in newIncoming:
                text = incoming.text
                parseText(text)
                parsedcount += 1


        elif len(incomingList) == parsedcount: 
            noUpdateCount += 1
            print("========= ", noUpdateCount, " ==========")
            print(oldUpdateCount)
            time.sleep(3)
        
        if noUpdateCount > 100:
            #close the while loop if there are no updates for 10 consecutive checks
            #10 is arbitrary, mostly made to close if its still running after the fresh incoming stop when market is closed
            #each one is around 4 seconds
            break

# repeatCommandList = ["/premium roku",]

# @tl.job(interval=timedelta(seconds=30, repeatCommandList=repeatCommandList))
# def repeatCommands(repeatCommandList):
#     repeatCommandList = ["/premium roku",]
#     enterCommands(repeatCommandList)


class Dataline:
    def __init__(self):
        self.size = ''
        self.root = ''
        self.expiry = ''
        self.strike = ''
        self.put_call = ''
        self.price = ''
        self.theo = ''
        self.sideplus = ''
        self.exch = ''
        self.time = ''
        self.ivol = ''
        self.ivol_chg = ''
        self.nbbo = ''
        self.cond = ''
        self.events = ''
        self.hilo = ''
        self.volume = ''
        self.open_int = ''
        self.delta = ''
        self.share_impact = ''
        self.dollar_impact = ''
        self.vega_dollars = ''
        self.baskets = ''
        self.label = ''
        self.usymbol = ''
        self.spot = ''
        self.detail = ''
        self.premium = ''
    
    
    
if __name__ == "__main__":
    setUp()
    login()
    time.sleep(3)
    commandList2 = ["/premium spy","+","/premium spy -1 ","+","/premium spy -2","+","/premium spy -3","+","/premium spy -4 ","+","/premium spy -5 ","+","/premium roku","+","/premium roku -1 ","+","/premium roku -2","+","/premium roku -3","+","/premium roku -4 ","+","/premium roku -5 ","+","/premium amzn","+","/premium amzn -1 ","+","/premium amzn -2","+","/premium amzn -3","+","/premium amzn -4 ","+","/premium amzn -5 ","+","/premium uber","+","/premium uber -1 ","+","/premium uber -2","+","/premium uber -3","+","/premium uber -4 ","+","/premium uber -5 ","+",]
    commandList3 = ["/premium spy -1 ", ]
    enterCommands(commandList3)
    # if __name__ == "__main__":
    #     repeatCommandList = repeatCommandList
    #     tl.start(block=True)
    getIncoming()
    #tl.start(block=True) #this should start the time loop to re-enter commands periodically 


##############################
#trying different methods of processing the data
dataLine = ">>200 SPY Jan22 14th 475 Calls trade $0.69 (CboeTheo=0.68)  ASKSIDE  [CBOE] 14:41:54.152 IV=15.5% +1.0 C2 440 x $0.68 - $0.69 x 568 C2  AUCTION  Vol=75k, OI=45k, Delta=22%, Impact=4431/$2.09m, Gamma=1076, Vega=$2142  ROHIT  SPY=470.69 Ref  Detail  "
tradeAlert = "<B>{size} {root} {expiry} {strike} {put_call} {price}</B> {theo} {side+} {exch} {time} <B>IV={ivol} {ivol_chg}</B> {nbbo} {cond} {events} {hilo} Vol={volume}, OI={open_int}, Delta={delta}, Impact={share_impact}/{dollar_impact}, Vega={vega_dollar} {baskets} {label} <B>{usymbol}={spot} {detail}</B>"
datalist = dataLine.split(' ')
datalist = [i for i in datalist if i]  #this removes the empty strings 