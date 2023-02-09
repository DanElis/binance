import asyncio
import threading
from collections import namedtuple, deque
from queue import Queue

from binance import AsyncClient, BinanceSocketManager

TradeInfo = namedtuple("SimpleTradeInfo", "price time")

ONE_HOUR_IN_MILLISECONDS = 3600000
ONE_HOUR_IN_SECONDS = 3600
RPS = 1000
THRESHOLD = 0.99


async def read_trade_info(symbol, queue_ti):
    client = await AsyncClient.create()
    bsm = BinanceSocketManager(client)
    async with bsm.trade_socket(symbol) as ts:
        while True:
            res = await ts.recv()
            queue_ti.put(res)


def find_max(last_ti: TradeInfo, list_trade_info: deque[TradeInfo]):
    # pass
    max_ti = TradeInfo(-1., 0)
    for ti in list_trade_info:
        if not can_it_be_max(last_ti, ti):
            continue
        if max_ti.price < ti.price:
            max_ti = ti.price
        elif max_ti.price == ti.price and ti.time > max_ti.time:
            max_ti = ti
    return max_ti


def can_it_be_max(last_ti: TradeInfo, ti: TradeInfo):
    if last_ti.time - ti.time > ONE_HOUR_IN_MILLISECONDS:
        return True
    return False


def is_max(ti: TradeInfo, current_max: TradeInfo):
    if ti.price >= current_max.price:
        return True
    return False


def has_price_dropped(current_max_ti, ti):
    if current_max_ti.price * THRESHOLD >= ti.price:
        return True
    return False


def process_trade_info(symbol, queue_ti):
    list_trade_info: deque[TradeInfo] = deque(maxlen=ONE_HOUR_IN_SECONDS * RPS)

    current_max_ti = TradeInfo(-1., 0)
    while True:
        ti = queue_ti.get()
        ti = TradeInfo(float(ti['p']), int(ti['T']))
        if is_max(ti, current_max_ti):
            current_max_ti = ti
        elif can_it_be_max(ti, current_max_ti):
            current_max_ti = find_max(ti, list_trade_info)
        if has_price_dropped(current_max_ti, ti):
            print(f"Price dropped {symbol}", current_max_ti.price, ti.price, current_max_ti.price * THRESHOLD)
        # print('ti', ti, current_max_ti.price, current_max_ti.price * THRESHOLD)
        list_trade_info.append(ti)


def wrap_async_read_trade_info(symbol, queue_ti):
    asyncio.run(read_trade_info(symbol, queue_ti))


def main(symbol):
    queue_ti = Queue(RPS * 100)

    thread_process = threading.Thread(target=wrap_async_read_trade_info, args=(symbol, queue_ti))
    thread_process.start()

    thread_read = threading.Thread(target=process_trade_info, args=(symbol, queue_ti))
    thread_read.start()

    thread_process.join()
    thread_read.join()


if __name__ == '__main__':
    main('XRPUSDT')
    # from multiprocessing import Pool
    #
    # with Pool(3) as p:
    #     print(p.map(main, ["XRPUSDT", "BTCUSDT", "ETHUSDT"]))
