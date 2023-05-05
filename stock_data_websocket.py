import websocket  # You can use either "python3 setup.py install" or "pip3 install websocket-client"
import json
import websocket._abnf 
from save_stock_data import save_minute_data
import datetime
# to install this library.

try:
    import thread
except ImportError:
    import _thread as thread
import time


endpoint = "ws://nimblewebstream.lisuns.com:4575/"
apikey = "ccbf345e-1ea5-4cba-9596-267bd539d37b"


def Authenticate(ws):
    print("Authenticating...")
    ws.send('{"MessageType":"Authenticate","Password":"' + apikey + '"}')


def SubscribeRealtime(ws):
    Exchange = "NFO"  # GFDL : Supported Values: NFO, NSE, NSE_IDX, CDS, MCX. Mandatory Parameter
    InstIdentifier = "NIFTY-I"  # GFDL : String of symbol name : NIFTY-I, RELIANCE, NIFTY 50, NATURALGAS-I, USDINR-I
    Unsubscribe = "false"  # GFDL : To stop data subscription for this symbol, send this value as "true"
    strMessage = '{"MessageType":"SubscribeRealtime","Exchange":"' + Exchange + '","Unsubscribe":"' + Unsubscribe + '","InstrumentIdentifier":"' + InstIdentifier + '"}'
    print('Message : ' + strMessage)
    ws.send(strMessage)


def SubscribeSnapshot(ws):
    ExchangeName = "NSE"  # GFDL : Supported Values: NFO, NSE, NSE_IDX, CDS, MCX. Mandatory Parameter
    InstIdentifier = "SBIN"  # GFDL : String of symbol name : NIFTY-I, RELIANCE, NIFTY 50, NATURALGAS-I, USDINR-I
    Periodicity = "MINUTE"  # GFDL : Supported values are : Minute, Hour
    Period = 1  # GFDL : Supported values are : 1,2,5,10,15,30 (for Minute Periodicity ONLY)
    Unsubscribe = "false"  # GFDL : To stop data subscription for this symbol, send this value as "true"
    strMessage = '{"MessageType":"SubscribeSnapshot","Exchange":"' + ExchangeName + '","InstrumentIdentifier":"' + InstIdentifier + '","Period":' + f'{Period}' + ',"Periodicity":"' + Periodicity + '","Unsubscribe":"' + Unsubscribe + '"}'
    print(strMessage)
    ws.send(strMessage)


def GetLastQuote(ws):
    ExchangeName = "NFO"  # GFDL : Supported Values: NFO, NSE, NSE_IDX, CDS, MCX. Mandatory Parameter
    InstIdentifier = "NIFTY-I"  # GFDL : String of symbol name : NIFTY-I, RELIANCE, NIFTY 50, NATURALGAS-I, USDINR-I
    isShortIdentifier = "false"  # GFDL : When using contractwise symbol like NIFTY20JULFUT,
    strMessage = '{"MessageType":"GetLastQuote","Exchange":"' + ExchangeName + '","isShortIdentifier":"' + isShortIdentifier + '","InstrumentIdentifier":"' + InstIdentifier + '"}'
    ws.send(strMessage)


def GetLastQuoteShort(ws):
    ExchangeName = "NFO"  # GFDL : Supported Values: NFO, NSE, NSE_IDX, CDS, MCX. Mandatory Parameter
    InstIdentifier = "NIFTY-I"  # GFDL : String of symbol name : NIFTY-I, RELIANCE, NIFTY 50, NATURALGAS-I, USDINR-I
    isShortIdentifier = "false"  # GFDL : When using contractwise symbol like NIFTY20JULFUT,
    strMessage = '{"MessageType":"GetLastQuoteShort","Exchange":"' + ExchangeName + '","isShortIdentifier":"' + isShortIdentifier + '","InstrumentIdentifier":"' + InstIdentifier + '"}'
    ws.send(strMessage)


def GetLastQuoteShortWithClose(ws):
    ExchangeName = "NFO"  # GFDL : Supported Values: NFO, NSE, NSE_IDX, CDS, MCX. Mandatory Parameter
    InstIdentifier = "NIFTY-I"  # GFDL : String of symbol name : NIFTY-I, RELIANCE, NIFTY 50, NATURALGAS-I, USDINR-I
    isShortIdentifier = "false"  # GFDL : When using contractwise symbol like NIFTY20JULFUT,
    strMessage = '{"MessageType":"GetLastQuoteShortWithClose","Exchange":"' + ExchangeName + '","isShortIdentifier":"' + isShortIdentifier + '","InstrumentIdentifier":"' + InstIdentifier + '"}'
    ws.send(strMessage)


def GetLastQuoteArray(ws):
    ExchangeName = "NFO"
    isShortIdentifiers = "false"  # GFDL : When using contractwise symbol like NIFTY20JULFUT,
    InstrumentIdentifiers = '[{"Value":"NIFTY-I"},{"Value":"BANKNIFTY-I"}]'
    strMessage = '{"MessageType":"GetLastQuoteArray","Exchange":"' + ExchangeName + '","isShortIdentifier":"' + isShortIdentifiers + '","InstrumentIdentifiers":' + InstrumentIdentifiers + '}'
    ws.send(strMessage)


def GetLastQuoteArrayShort(ws):
    ExchangeName = "NFO"
    isShortIdentifiers = "false"  # GFDL : When using contractwise symbol like NIFTY20JULFUT,
    InstrumentIdentifiers = '[{"Value":"NIFTY-I"},{"Value":"BANKNIFTY-I"}]'
    strMessage = '{"MessageType":"GetLastQuoteArrayShort","Exchange":"' + ExchangeName + '","isShortIdentifier":"' + isShortIdentifiers + '","InstrumentIdentifiers":' + InstrumentIdentifiers + '}'
    ws.send(strMessage)


def GetLastQuoteArrayShortWithClose(ws):
    ExchangeName = "NSE"
    isShortIdentifiers = "false"  # GFDL : When using contractwise symbol like NIFTY20JULFUT,
    InstrumentIdentifiers = '[{"Value":"WIPRO"},{"Value":"RELIANCE"}]'
    strMessage = '{"MessageType":"GetLastQuoteArrayShortWithClose","Exchange":"' + ExchangeName + '","isShortIdentifier":"' + isShortIdentifiers + '","InstrumentIdentifiers":' + InstrumentIdentifiers + '}'
    ws.send(strMessage)


def GetSnapshot(ws):
    ExchangeName = "NSE"  # GFDL : Supported Values : NFO, NSE, NSE_IDX, CDS, MCX. Mandatory Parameter
    Periodicity = "MINUTE"  # GFDL : Supported Values : Minute, Hour
    Period = 1  # GFDL : Supported Values : 1,2,3,5,10,15,20,30
    InstrumentIdentifiers = '[{"Value":"WIPRO"},{"Value":"RELIANCE"}]'
    isShortIdentifiers = "false"  # GFDL : When using contractwise symbol like NIFTY20JULFUT,
    strMessage = '{"MessageType":"GetSnapshot","Exchange":"' + ExchangeName + '","Periodicity":"' + Periodicity + '","Period":' + f'{Period}' + ',"isShortIdentifiers":"' + isShortIdentifiers + '","InstrumentIdentifiers":' + InstrumentIdentifiers + '}'
    ws.send(strMessage)


def GetHistory(ws):
    ExchangeName = "NSE"  # GFDL : Supported Values : NFO, NSE, NSE_IDX, CDS, MCX. Mandatory Parameter
    InstIdentifier = "SBIN"  # GFDL : String of symbol name : NIFTY-I, RELIANCE, NIFTY 50, NATURALGAS-I, USDINR-I
    Periodicity = "MINUTE"  # GFDL : [“TICK”]/[“MINUTE”]/[“HOUR”]/[“DAY”]/[“WEEK”]/[“MONTH”], default = [“TICK”]
    Period = 1  # GFDL : Numerical value 1, 2, 3…, default = 1
    # From = 1615919400         #GFDL : Numerical value of UNIX Timestamp like ‘1388534400’
    # To = 1621171693           #GFDL : Numerical value of UNIX Timestamp like ‘1388534400’
    Max = 100  # GFDL : Numerical value of number of rrecords to be return
    UserTag = "User1"  # GFDL : String hich will be returned in response
    isShortIdentifier = "true"  # GFDL : Represent the format of InstIdentifier if shor or long
    strMessage = '{"MessageType":"GetHistory","Exchange":"' + ExchangeName + '","InstrumentIdentifier":"' + InstIdentifier + '","Periodicity":"' + Periodicity + '","Period":"' + str(
        Period) + '","Max":"' + str(
        Max) + '","UserTag":"' + UserTag + '","isShortIdentifier":"' + isShortIdentifier + '"}'
    print('Message : ' + strMessage)
    ws.send(strMessage)


def GetInstrumentsOnSearch(ws):
    ExchangeName = "NFO"  # GFDL : Supported Values : NFO, NSE, NSE_IDX, CDS, MCX. Mandatory Parameter
    Search = "NIFTY"  # GFDL : This is the search string
    Product = "NIFTY"  # GFDL : Optional argument to filter the search by products like NIFTY, RELIANCE, etc.
    InstrumentType = "FUTIDX"  # GFDL : Optional argument to filter the search by products like FUTIDX,FUTSTK,OPTIDX,OPTSTK,FUTCUR, FUTCOM, etc.
    # OptionType = "PE"			#GFDL : Optional argument to filter the search by OptionTypes like CE, PE
    # Expiry = "29APR2021"	    #GFDL : Optional argument to filter the search by Expiry like 29APR2021
    # StrikePrice = 10000 	    #GFDL : Optional argument to filter the search by Strike Price like 10000, 75.5, 1250, etc.
    # OnlyActive = "TRUE"	    #GFDL : Optional argument (default=True) to control returned data. If false,
    strMessage = '{"MessageType":"GetInstrumentsOnSearch","Exchange":"' + ExchangeName + '","Search":"' + Search + '"}'
    ws.send(strMessage)


def GetInstruments(ws):
    ExchangeName = "NFO"
    # InstrumentType = "FUTSTK"	#GFDL : Optional argument to filter the search by products like FUTIDX, FUTSTK, OPTIDX, OPTSTK,
    # FUTCUR, FUTCOM, etc.
    # Product = "NIFTY"			#GFDL : Optional argument to filter the search by products like NIFTY, RELIANCE, etc.
    # OptionType = "PE"		    #GFDL : Optional argument to filter the search by OptionTypes like CE, PE
    # Expiry = "29APR2021"	    #GFDL : Optional argument to filter the search by Expiry like 29APR2021
    # StrikePrice = 10000	    	#GFDL : Optional argument to filter the search by Strike Price like 10000, 75.5, 1250, etc.
    # OnlyActive = "TRUE"   		#GFDL : Optional argument (default=True) to control returned data. If false,
    #       even expired contracts are returned
    strMessage = '{"MessageType":"GetInstruments","Exchange":"' + ExchangeName + '"}'
    ws.send(strMessage)


def GetServerInfo(ws):
    strMessage = '{"MessageType":"GetServerInfo"}'
    ws.send(strMessage)


def GetExchanges(ws):
    strMessage = '{"MessageType":"GetExchanges"}'
    ws.send(strMessage)


def GetInstrumentTypes(ws):
    ExchangeName = "MCX"
    strMessage = '{"MessageType":"GetInstrumentTypes","Exchange":"' + ExchangeName + '"}'
    ws.send(strMessage)


def GetProducts(ws):
    ExchangeName = "NFO"
    strMessage = '{"MessageType":"GetProducts","Exchange":"' + ExchangeName + '"}'
    ws.send(strMessage)


def GetExpiryDates(ws):
    ExchangeName = "NFO"
    strMessage = '{"MessageType":"GetExpiryDates","Exchange":"' + ExchangeName + '"}'
    ws.send(strMessage)


def GetOptionTypes(ws):
    ExchangeName = "NFO"
    strMessage = '{"MessageType":"GetOptionTypes","Exchange":"' + ExchangeName + '"}'
    ws.send(strMessage)


def GetStrikePrices(ws):
    ExchangeName = "NFO"
    strMessage = '{"MessageType":"GetStrikePrices","Exchange":"' + ExchangeName + '"}'
    ws.send(strMessage)


def GetLimitation(ws):
    strMessage = '{"MessageType":"GetLimitation"}'
    ws.send(strMessage)


def GetMarketMessages(ws):
    ExchangeName = "NFO"
    strMessage = '{"MessageType":"GetMarketMessages","Exchange":"' + ExchangeName + '"}'
    ws.send(strMessage)


def GetExchangeMessages(ws):
    ExchangeName = "NSE"
    strMessage = '{"MessageType":"GetExchangeMessages","Exchange":"' + ExchangeName + '"}'
    ws.send(strMessage)


def GetLastQuoteOptionChain(ws):
    Exchange = "NFO"  # GFDL : Supported Values : NFO, NSE, NSE_IDX, CDS, MCX. Mandatory Parameter
    Product = "RELIANCE"  # GFDL : Mandatory Parameter. Example, RELIANCE, BANKNIFTY, NIFTY
    # Expiry = "23JAN2020"  # GFDL : Optional field, in DDMMMYYYY format. If absent, result is sent for all active Expiries
    # OptionType = "CE"  # GFDL : Optional field, CE or PE. If absent, result is sent for all Option Types
    # StrikePrice = 10000  # GFDL : Optional field, as a number. If absent, result is sent for all strike prices
    strMessage = '{"MessageType":"GetLastQuoteOptionChain","Exchange":"' + Exchange + '","Product":"' + Product + '"}'
    ws.send(strMessage)


def GetExchangeSnapshot(ws):
    Exchange = "NFO"  # GFDL : Supported Values : NFO, NSE, NSE_IDX, CDS, MCX. Mandatory Parameter
    Periodicity = "Minute"  # GFDL : Mandatory Parameter. Supported Values : Minute, Hour. Default = Minute
    Period = 1  # GFDL : Mandatory Parameter. Supported Values : 1,2,5,10,15,30. Default = 1
    # InstrumentType = "FUTIDX"  # GFDL : Optional Parameter. FUTIDX, FUTSTK, OPTIDX, OPTSTK, FUTCOM, FUTCUR, etc.
    #From = From  # GFDL : Epoch value of time in seconds since 1st January 1970. For example, 1567655100 is
    #To = To  # GFDL : Epoch value of time in seconds since 1st January 1970. For example, 1567655100 is
    nonTraded = "true"  # GFDL : true/false. When true, results are sent with data of even non traded instruments.
    # strMessage = '{"MessageType":"GetExchangeSnapshot","nonTraded":"true","Exchange":"' + Exchange + '","Period":' + f'{Period}' + ',"Periodicity":"' + Periodicity + ',"From:" + From + }'
    
    strMessage = '{"MessageType":"GetExchangeSnapshot", "Exchange":"' + Exchange + '","Periodicity":"' + Periodicity + '","nonTraded":' + nonTraded + ', "Period":' + f'{Period}' + '}';
    ws.send(strMessage)

    


def GetExchangeSnapshotAfterMarket(ws):
    Exchange = "MCX"  # GFDL : Supported Values : NFO, NSE, NSE_IDX, CDS, MCX. Mandatory Parameter
    Periodicity = "Minute"  # GFDL : Mandatory Parameter. Supported Values : Minute, Hour. Default = Minute
    Period = 1  # GFDL : Mandatory Parameter. Supported Values : 1,2,5,10,15,30. Default = 1
    # InstrumentType = "FUTIDX"  # GFDL : Optional Parameter. FUTIDX, FUTSTK, OPTIDX, OPTSTK, FUTCOM, FUTCUR, etc.
    xDate = 1627291664  # GFDL : Epoch value of time in seconds since 1st January 1970. For example, 1567655100 is
    # To: 0  # GFDL : Epoch value of time in seconds since 1st January 1970. For example, 1567655100 is
    # nonTraded = "false"  # GFDL : true/false. When true, results are sent with data of even non traded instruments.
    strMessage = '{"MessageType":"GetExchangeSnapshotAfterMarket","Exchange":"' + Exchange + '","Period":' + f'{Period}' + ',"Periodicity":"' + Periodicity + '","Date":"' + str(
        xDate) + '"}'
    ws.send(strMessage)


def GetLastQuoteOptionGreeks(ws):
    Exchange = "NFO"  # GFDL : Supported Values : NFO, NSE, NSE_IDX, CDS, MCX. Mandatory Parameter
    Token = "53937"  # GFDL : Mandatory Parameter. Supported Values : Minute, Hour. Default = Minute
    strMessage = '{"MessageType":"GetLastQuoteOptionGreeks","Exchange":"' + Exchange + '","Token":"' + str(Token) + '"}'
    ws.send(strMessage)


def GetLastQuoteArrayOptionGreeks(ws):
    Exchange = "NFO"  # GFDL : Supported Values : NFO, NSE, NSE_IDX, CDS, MCX. Mandatory Parameter
    Tokens = '[{"Value":"53939"},{"Value":"53941"}]'  # GFDL : Mandatory Parameter. Supported Values : Minute, Hour. Default = Minute
    strMessage = '{"MessageType":"GetLastQuoteArrayOptionGreeks","Exchange":"' + Exchange + '","Tokens":' + Tokens + '}'
    print(strMessage)
    ws.send(strMessage)

def GetLastQuoteOptionGreeksChain(ws):
    Exchange = "NFO"  # GFDL : Supported Values : NFO, NSE, NSE_IDX, CDS, MCX. Mandatory Parameter
    Product = "NIFTY"  # GFDL : Mandatory Parameter. Supported Values : Minute, Hour. Default = Minute
    strMessage = '{"MessageType":"GetLastQuoteOptionGreeksChain","Exchange":"' + Exchange + '","Product":"' + Product + '"}'
    print(strMessage)
    ws.send(strMessage)

def on_message(ws, message):

    data = json.loads(message)

    if data['MessageType'] == 'ExchangeSnapshotResult' :
        # data = data.get('Result')
        print("---------------------------------------------------------------------------------------")
        if len(data) > 1 :
            total_length = 0

            for result in data['Result']:
                total_length += len(result['Result'])
                save_minute_data(result['Result'])
                # print(result['Result'])

            print("Response : " , total_length)

            ws.close();


    # Authenticate : {"Complete":true,"Message":"Welcome!","MessageType":"AuthenticateResult"}
    allures = message.split(',')
    strComplete = allures[0].split(':')
    result = str(strComplete[1])
    # print('Response : ' + result)
    if result == "true":
        print('AUTHENTICATED!!!')
        # SubscribeRealtime(ws)  # GFDL : Subscribes to realtime data (server will push new data whenever available)
        # SubscribeSnapshot(ws)                  # GFDL : Subscribes to minute snapshot data (server will push new data whenever available)

        # GetLastQuote(ws)                       #GFDL : Returns LastTradePrice of Single Symbol (detailed)
        # GetLastQuoteShort(ws)                  # GFDL : Returns LastTradePrice of Single Symbol (short)
        # GetLastQuoteShortWithClose(ws)         #GFDL : Returns LastTradePrice of Single Symbol (short) with Close of Previous Day

        # GetLastQuoteArray(ws)                  #GFDL : Returns LastTradePrice of multiple Symbols – max 25 in single call (detailed)
        # GetLastQuoteArrayShort(ws)             #GFDL : Returns LastTradePrice of multiple Symbols – max 25 in single call (short)
        # GetLastQuoteArrayShortWithClose(ws)    #GFDL : Returns LastTradePrice of multiple Symbols – max 25 in single call (short) with Previous Close

        # GetSnapshot(ws)                        #GFDL : Returns latest Snapshot Data of multiple Symbols – max 25 in single call
        # GetHistory(ws)                         #GFDL : Returns historical data (Tick / Minute / EOD)

        # GetInstrumentsOnSearch(ws)             #GFDL : Returns array of max. 20 instruments by selected exchange and 'search string'
        # GetInstruments(ws)                     #GFDL : Returns array of instruments by selected exchange

        # GetServerInfo(ws)                      #GFDL : Returns the server endpoint where user is connected
        # GetExchanges(ws)                       #GFDL : Returns array of available exchanges configured for API Key
        # GetInstrumentTypes(ws)                 #GFDL : Returns list of Instrument Types (e.g. FUTIDX, FUTSTK, etc.)
        # GetProducts(ws)                        #GFDL : Returns list of Products (e.g. NIFTY, BANKNIFTY, GAIL, etc.)
        # GetExpiryDates(ws)                     #GFDL : Returns array of Expiry Dates (e.g. 25JUN2020, 30JUL2020, etc.)
        # GetOptionTypes(ws)                     #GFDL : Returns list of Option Types (e.g. CE, PE, etc.)
        # GetStrikePrices(ws)                    #GFDL : Returns list of Strike Prices (e.g. 10000, 11000, 75.5, etc.)
        # GetLimitation(ws)                      #GFDL : Returns user account information (functions allowed, Exchanges allowed, symbol limit, etc.)

        # GetMarketMessages(ws)                  #GFDL : Returns array of last messages (Market Messages) related to selected exchange
        # GetExchangeMessages(ws)				#GFDL : Returns array of last messages (Exchange Messages) related to selected exchange

        # GetLastQuoteOptionChain(ws)			    #GFDL : Returns OptionChain data in realtime
     
        GetExchangeSnapshot(ws)                   #GFDL : Returns entire Exchange Snapshot in realtime
        
        # GetExchangeSnapshotAfterMarket(ws)        #GFDL : Returns entire Exchange Snapshot after market.

        # GetLastQuoteOptionGreeks(ws)              #GFDL : Returns Last Traded Option Greek values of Single Symbol
        # GetLastQuoteArrayOptionGreeks(ws)         #GFDL : Returns Last Traded Option Greek values of multiple Symbols – max 25 in single call
        # GetLastQuoteOptionGreeksChain(ws)         #GFDL : Returns Last Traded Option Greek values of entire OptionChain of requested underlying

def on_error(ws, error):
    print("Error",error)


def on_close(ws):
    print("Reconnecting...")
    websocket.setdefaulttimeout(30)
    websocket._abnf.MAX_MESSAGE_SIZE = 2**64-1  # set maximum message size to 2^64-1 bytes
    ws = websocket.WebSocket()
    ws.connect(endpoint)


def on_open(ws):
    # print("Connected...")
     # WebSocket connection is established
    ws.sock.max_size = 1024*1024*20

    def run(*args):
        time.sleep(1)
        Authenticate(ws)

    thread.start_new_thread(run, ())


if __name__ == "__main__":
    websocket.enableTrace(False)
    ws = websocket.WebSocketApp(endpoint,
                                on_open=on_open,
                                on_message=on_message,
                                on_error=on_error,
                                on_close=on_close)

    ws.run_forever()
