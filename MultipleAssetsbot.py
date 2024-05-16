import os
import logging
import asyncio
from binance.client import Client
from binance.enums import *
from binance.exceptions import BinanceAPIException, BinanceOrderException
from requests.exceptions import RequestException
from binance import AsyncClient, BinanceSocketManager

# Initialize Binance API client 
api_key = os.getenv('BINANCE_API_KEY')
api_secret = os.getenv('BINANCE_API_SECRET')
client = Client(api_key, api_secret)

# Setup logging
logging.basicConfig(filename='arbitrage_bot.log', level=logging.INFO, 
                    format='%(asctime)s - %(levelname)s - %(message)s')

# Configuration
MAX_RETRIES = 5
RETRY_DELAY = 2  # Seconds

# Fee rates 
SPOT_FEE_RATE = 0.075 / 100
FUTURES_FEE_RATE = 0.05 / 100

# Stop-loss and take-profit percentages
STOP_LOSS_PERCENTAGE = 1  # 1%
TAKE_PROFIT_PERCENTAGE = 2  # 2%

# Variables to store real-time prices for multiple pairs
prices = {}
# Dictionary to track open positions
open_positions = {}

async def fetch_tradable_pairs(client):
    try:
        # Fetch spot market pairs
        spot_info = await client.get_exchange_info()
        spot_pairs = {item['symbol'] for item in spot_info['symbols'] if item['status'] == 'TRADING'}
        
        # Fetch futures market pairs
        futures_info = await client.futures_exchange_info()
        futures_pairs = {item['symbol'] for item in futures_info['symbols'] if item['status'] == 'TRADING'}
        
        # Find common pairs
        tradable_pairs = spot_pairs.intersection(futures_pairs)
        return tradable_pairs
    except Exception as e:
        logging.error(f"Error fetching tradable pairs: {e}")
        return set()

def calculate_spread(spot_price, futures_price):
    return futures_price - spot_price

def calculate_quantity(usdt_balance, btc_price):
    return usdt_balance / btc_price

async def get_balance(client, asset):
    for _ in range(MAX_RETRIES):
        try:
            balance_info = await client.get_asset_balance(asset=asset)
            return float(balance_info['free'])
        except (BinanceAPIException, RequestException) as e:
            logging.error(f"Error fetching balance for {asset}: {e}")
            await asyncio.sleep(RETRY_DELAY)
    return None

async def place_spot_order(client, symbol, quantity, side, order_type=ORDER_TYPE_MARKET, price=None):
    for _ in range(MAX_RETRIES):
        try:
            if order_type == ORDER_TYPE_MARKET:
                order = await client.order_market(symbol=symbol, side=side, quantity=quantity)
            else:
                order = await client.order_limit(symbol=symbol, side=side, quantity=quantity, price=price, timeInForce=TIME_IN_FORCE_GTC)
            return order
        except (BinanceAPIException, BinanceOrderException) as e:
            logging.error(f"Error placing spot order: {e}")
            await asyncio.sleep(RETRY_DELAY)
    return None

async def place_futures_order(client, symbol, quantity, side, order_type=ORDER_TYPE_MARKET, price=None):
    for _ in range(MAX_RETRIES):
        try:
            if order_type == ORDER_TYPE_MARKET:
                order = await client.futures_create_order(symbol=symbol, side=side, type='MARKET', quantity=quantity)
            else:
                order = await client.futures_create_order(symbol=symbol, side=side, type='LIMIT', quantity=quantity, price=price, timeInForce='GTC')
            return order
        except (BinanceAPIException, BinanceOrderException) as e:
            logging.error(f"Error placing futures order: {e}")
            await asyncio.sleep(RETRY_DELAY)
    return None

async def process_message(msg, msg_type, symbol):
    global prices
    if msg_type == 'spot':
        prices[symbol]['spot'] = float(msg['p'])
    elif msg_type == 'futures':
        prices[symbol]['futures'] = float(msg['p'])

    spot_price = prices[symbol].get('spot')
    futures_price = prices[symbol].get('futures')

    if spot_price is not None and futures_price is not None:
        logging.info(f"{symbol} - Spot Price: {spot_price}, Futures Price: {futures_price}")
        spread = calculate_spread(spot_price, futures_price)
        logging.info(f"{symbol} - Spread: {spread}")

        # Calculate fees
        spot_fee = spot_price * SPOT_FEE_RATE
        futures_fee = futures_price * FUTURES_FEE_RATE
        total_fees = spot_fee + futures_fee

        potential_profit = spread - total_fees
        prices[symbol]['potential_profit'] = potential_profit

async def close_position(client, symbol, quantity):
    # Close spot position
    await place_spot_order(client, symbol, quantity, SIDE_SELL)

    # Close futures position
    await place_futures_order(client, symbol, quantity, SIDE_BUY)

async def monitor_positions(client):
    global open_positions, prices

    while True:
        for symbol, position in list(open_positions.items()):
            spot_price = prices[symbol].get('spot')
            futures_price = prices[symbol].get('futures')

            if spot_price is None or futures_price is None:
                continue

            initial_spread = position['initial_spread']
            current_spread = calculate_spread(spot_price, futures_price)
            profit = current_spread - initial_spread

            logging.info(f"{symbol} - Monitoring position. Profit: {profit}")

            if profit >= initial_spread * (TAKE_PROFIT_PERCENTAGE / 100):
                logging.info(f"{symbol} - Take profit target reached. Closing position.")
                await close_position(client, symbol, position['quantity'])
                del open_positions[symbol]

            if profit <= initial_spread * (STOP_LOSS_PERCENTAGE / 100):
                logging.info(f"{symbol} - Stop loss target reached. Closing position.")
                await close_position(client, symbol, position['quantity'])
                del open_positions[symbol]

        await asyncio.sleep(5)

async def main():
    global client, prices
    client = await AsyncClient.create(api_key, api_secret)
    bm = BinanceSocketManager(client)

    # Fetch tradable pairs
    tradable_pairs = await fetch_tradable_pairs(client)
    logging.info(f"Tradable pairs: {tradable_pairs}")

    # Initialize prices dictionary
    prices = {symbol: {} for symbol in tradable_pairs}

    # Start WebSocket connections for each pair
    sockets = []
    for symbol in tradable_pairs:
        spot_socket = bm.trade_socket(symbol.lower())
        futures_socket = bm.futures_trade_socket(symbol.lower())
        sockets.append((spot_socket, 'spot', symbol))
        sockets.append((futures_socket, 'futures', symbol))

    async def handle_sockets(sockets):
        tasks = [handle_socket(socket, msg_type, symbol) for socket, msg_type, symbol in sockets]
        await asyncio.gather(*tasks)

    async def handle_socket(socket, msg_type, symbol):
        async with socket as stream:
            while True:
                msg = await stream.recv()
                await process_message(msg, msg_type, symbol)

    # Run WebSocket handlers
    asyncio.create_task(handle_sockets(sockets))
    asyncio.create_task(monitor_positions(client))

    # Continuously monitor for arbitrage opportunities
    while True:
        best_pair = None
        best_profit = 0

        for symbol, data in prices.items():
            potential_profit = data.get('potential_profit', 0)
            if potential_profit > best_profit:
                best_profit = potential_profit
                best_pair = symbol

        if best_pair and best_profit > 0:
            logging.info(f"Best pair to trade: {best_pair} with potential profit: {best_profit}")

            spot_price = prices[best_pair]['spot']
            futures_price = prices[best_pair]['futures']

            # Check balances
            usdt_balance = await get_balance(client, 'USDT')
            if usdt_balance is None or usdt_balance < spot_price:
                logging.warning(f"{best_pair} - Insufficient USDT balance for spot trade.")
                await asyncio.sleep(10)
                continue

            btc_balance = await get_balance(client, 'BTC')
            if btc_balance is None or btc_balance < calculate_quantity(usdt_balance, spot_price):
                logging.warning(f"{best_pair} - Insufficient BTC balance for futures trade.")
                await asyncio.sleep(10)
                continue

            # Calculate quantity based on available USDT balance
            quantity = calculate_quantity(usdt_balance, spot_price)

            # Place spot and futures orders
            spot_order = await place_spot_order(client, best_pair, quantity, SIDE_BUY)
            futures_order = await place_futures_order(client, best_pair, quantity, SIDE_SELL)

            if spot_order and futures_order:
                logging.info(f"{best_pair} - Spot Order: {spot_order}")
                logging.info(f"{best_pair} - Futures Order: {futures_order}")

                # Track the open position
                initial_spread = calculate_spread(spot_price, futures_price)
                open_positions[best_pair] = {
                    'quantity': quantity,
                    'initial_spread': initial_spread
                }

            await asyncio.sleep(10)  # Adjust the sleep time 

        await asyncio.sleep(5)  # Adjust the polling interval 

if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
