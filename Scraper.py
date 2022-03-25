from selenium import webdriver
import time
from selenium.webdriver.common.keys import Keys
from selenium.webdriver import ActionChains
from w3lib.html import remove_tags
import re
import pyodbc


from regex import *

global incomingCount, noUpdateCount


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


def getIncoming():
    incomingCount = 0
    noUpdateCount = 0
    incomingList = driver.find_elements_by_xpath('//div[@class="incoming"]') 
    incomingCount = len(incomingList)
    print("incoming count: ", incomingCount)
    for incoming in incomingList:
        #this will process the incomings that come in from & during the entering of the premium commands 
        text = incoming.text
        print("in original incomings")
        
        if text.count(">>") >1: 
            #this checks if its a return from /premium and splits it if so 
            print("premium line ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~\n")
            print(text)
            text = text.split(">>")
            text = text[1:]
            #this processes the main part of the premium return, the first part (pre->>) can be accessed with text[0] here
            for line in text:
                try:
                    print(line)
                    matches = regexLine(regexPremium, line) 
                    print(matches)
                    if matches == {}:
                        print("no matches found in line - line 86")
                        continue
                    else:
                        time.sleep(3)
                        cleanedData = premiumCleaner(matches)
                        vals = premiumAssembler(cleanedData)
                        sqlInserter(vals)
                except KeyError:
                    print("key error - line 94")
        else:
            print(text)
            matches = regexLine(regex, text)
            print(matches)
            if matches == {}:
                print("No matches found - line 104")
            else:
                time.sleep(3)
                try:
                    cleanedData = dataCleaner(matches)
                    vals = assembler(cleanedData)
                    sqlInserter(vals)
                except KeyError:
                    print("key error")
        
        print("\n incoming alert: ", text, "\n")
        
    print("\n finished original incomings \n")
    while True:
        #this will continually look for new incomings after completing the initial ones 
        incomingList = driver.find_elements_by_xpath('//div[@class="incoming"]')
        if len(incomingList) > incomingCount:
            noUpdateCount = 0
            incomingDiff = 0 - (len(incomingList) - incomingCount)
            incomingCount = len(incomingList)
            newIncoming = incomingList[incomingDiff: -1]
            for incoming in newIncoming:
                #parse(incoming)
                print("*****new incomings: \n", incomingDiff, " \n*****\n")
                incomingDiff += 1
                line = incoming.text
                matches = regexLine(regex, line)
                if matches == {}:
                    print(line)
                    print("No match found")
                else:
                    time.sleep(1)
                    cleanedData = dataCleaner(matches)
                    vals = assembler(cleanedData)
                    sqlInserter(vals)


        elif len(incomingList) == incomingCount:
            noUpdateCount += 1
            print("========= ", noUpdateCount, " ==========")
            time.sleep(3)
        if noUpdateCount > 10:
            #close the while loop if there are no updates for 10 consecutive checks
            #10 is arbitrary, mostly made to close if its still running after the fresh incoming stop when market is closed
            #each one is around 3 seconds
            break

setUp()
login()
time.sleep(3)
enterCommands(commandList)
getIncoming()



