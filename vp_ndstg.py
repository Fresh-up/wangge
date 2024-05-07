from __future__ import (absolute_import, division, print_function,
                        unicode_literals)

import datetime  # For datetime objects
import pandas as pd
import backtrader as bt
import backtrader.feeds as btfeeds
import csv
import numpy as np
from datetime import timedelta
import random
import backtrader.indicators as btind
import backtrader.analyzers as btanalyzers
from backtrader.utils.autodict import AutoOrderedDict
import matplotlib.pyplot as plt
from IPython.display import display
from queue import Queue
import matplotlib.pyplot as plt
import matplotlib.ticker as ticker


def printg(*args):
    for arg in args:
        print("\033[96m", arg, "\033[0m")
def printy(*args):
    for arg in args:
        print("\033[33m", arg, "\033[0m")

def autodict_to_dict(auto_ordered_dict):
    if isinstance(auto_ordered_dict, AutoOrderedDict):
        # 递归地将所有嵌套的 AutoOrderedDict 转换为普通字典
        return {key: autodict_to_dict(value) for key, value in auto_ordered_dict.items()}
    else:
        # 如果不是 AutoOrderedDict，直接返回值
        return auto_ordered_dict

def print_statics(dd, rt, td_an):
    rt_dict = dict(rt.items())
    return_rate = round(rt_dict['rtot'], 5)
    # print('收益率为： %.3f %%' %(return_rate * 100))

    dd_dict = dict(dd.items())
    max_dd_dict = dict(dd_dict['max'])
    max_drawdown = round(max_dd_dict['drawdown'], 5)
    print('浮动最大回撤：%.3f %%' %(max_drawdown))

    td_an_dict = autodict_to_dict(td_an) # 以下都是不含当前开仓的
    
    td_total = td_an_dict['total']['closed'] # 交易总笔数(一买一卖算一次)
    pnl_gross = td_an_dict['pnl']['gross']['total'] # 毛利润
    pnl_net = td_an_dict['pnl']['net']['total'] # 净利润(去掉手续费的)
    rt_rate = pnl_net / 100000
    won_num = td_an_dict['won']['total'] # 盈利笔数
    max_profit = td_an_dict['won']['pnl']['max'] # 单笔最大盈利
    lost_num = td_an_dict['lost']['total'] # 亏损笔数
    max_lost = td_an_dict['lost']['pnl']['max'] # 单笔最大亏损
    long_num = td_an_dict['long']['total'] # 开多次数
    short_num = td_an_dict['short']['total'] # 开空次数
    winning_rate = won_num / (won_num + lost_num)

    print('实际收益率： %.3f %%' %(rt_rate * 100))
    print('交易总笔数: ', td_total)
    print('毛利润: ', pnl_gross)
    print('净利润: ', pnl_net)
    print('盈利笔数: ', won_num)
    print('单笔最大盈利: ', max_profit)
    print('亏损笔数: ', lost_num)
    print('单笔最大亏损: ', max_lost)
    print('开多次数: ', long_num)
    print('开空次数: ', short_num)
    print('胜率: ', winning_rate)

    PLratio = td_an_dict['won']['pnl']['average'] / (td_an_dict['lost']['pnl']['average'] + 1e-7) # 盈亏比
    print('盈亏比: ', PLratio)

    return [return_rate, max_drawdown, td_total, pnl_gross, pnl_net, won_num, max_profit, lost_num, 
            max_lost, long_num, short_num, winning_rate, PLratio]


class TestStg(bt.Strategy):
    params = (('boll_period', 20),
              ('stake', 600),
              ('ma_period', 10),
              ('multi', 1),
              ('fudu', 0.005))
    
    
    def log(self, txt, dt=None):
        dt = dt or self.data.datetime[0]
        if isinstance(dt, float):
            dt = bt.num2date(dt)
        print('%s, %s' % (dt.isoformat(), txt))
    
    def cancel_all_orders(self):
        for order in self.active_orders[:]:  
            self.cancel(order)
        self.active_orders = []  

    def notify_order(self, order):
        if order.status in [order.Submitted, order.Accepted]:
            self.order = order
            self.active_orders.append(order)
            return

        if order.status in [order.Expired]:
            self.log('BUY EXPIRED')

        if order.status in [order.Cancelled]:
            self.log('ORDER CANCELLED')

        if order.status in [order.Completed]:
            if order.isbuy():
                self.log(
                    'BUY EXECUTED, Price: %.2f, Cost: %.2f, Comm %.2f' %
                    (order.executed.price,
                     order.executed.value,
                     order.executed.comm))

            else:  # Sell
                self.log('SELL EXECUTED, Price: %.2f, Cost: %.2f, Comm %.2f' %
                         (order.executed.price,
                          order.executed.value,
                          order.executed.comm))

        self.order = None
    
    def notify_trade(self, trade):
        if not trade.isclosed:
            return

        self.log("交易收益：毛利 %.2f 净利：%.2f" % (trade.pnl, trade.pnlcomm))
            

    def __init__(self):
        self.clos = self.datas[0].close
        self.open = self.datas[0].open
        self.high = self.datas[0].high
        self.low = self.datas[0].low
        self.vol = self.datas[0].volume
        self.poc = self.datas[0].poc
        self.skew = self.datas[0].skew
        self.skew_up = self.datas[0].skew_up
        self.skew_down = self.datas[0].skew_down
        self.order = None

        self.active_orders = []  # 存储活跃订单的列表
        self.bbands = btind.BollingerBands(period=self.p.boll_period)
        self.poc_at_tail = (self.poc - self.low) / ((self.high - self.low) + 0.000000001)
        self.poc_at_head = (self.high - self.poc) / ((self.high - self.low) + 0.000000001)
        self.klen = btind.SMA((self.high - self.low), period=self.p.ma_period)
        self.df = []

        self.bodysize = abs(self.clos - self.open)
        # self.lowwicksizeabs = min(self.clos, self.open) - self.low
        self.roc = (self.clos - self.open) / self.open

    def next(self):
        self.cancel_all_orders()

        if self.poc_at_tail < 0.15 and self.poc_at_tail > 0.1 and self.skew[0] > 0 and self.open[0] < self.bbands.mid[0]:
            self.buy(size=self.p.stake, price=self.poc[0], exectype=bt.Order.Limit)
            
        # 接针
        if (min(self.clos[0], self.open[0]) - self.low[0]) > self.bodysize[0] * self.p.multi and self.roc[0] <= -self.p.fudu:
            self.buy(size=self.p.stake, price=self.clos[0], exectype=bt.Order.Limit)
        if self.high[0] > self.bbands.top[0] and self.getposition().size > 0 and self.getposition().price < self.bbands.top[0]:
            self.sell(size=self.getposition().size, price=self.bbands.top[0], exectype=bt.Order.Limit)

    def stop(self):
        printy('stop')
        print('仓位： ', self.getposition().size)
        # plt.bar(range(len(self.df)), self.df)
        # for order in self.active_orders:
        #     print('Order:', order.ref, 'Status:', order.getstatusname())

if __name__ in '__main__':
    pth = 'BN_trades_data/WLDUSDTkbar0310~0413.csv'
    cerebro = bt.Cerebro()

    class My_CSVData(bt.feeds.GenericCSVData):
        lines = ('poc', 'skew', 'skew_up', 'skew_down', )
        params = (
        ('fromdate', datetime.datetime(2024, 3, 9)),
        ('todate', datetime.datetime(2024, 4, 13)),
        ('nullvalue', 0.0),
        ('dtformat', ('%Y-%m-%d')),
        ('tmformat', ('%H:%M:%S')),
        ('datetime', 14),
        ('time', 15),
        ('high', 1),
        ('low', 2),
        ('open', 0),
        ('close', 3),
        ('volume', 4),
        ('poc', 8),
        ('skew', 10),
        ('skew_up', 11),
        ('skew_down', 12),
        ('openinterest', -1)
    )

    data = My_CSVData(dataname=pth)

    cerebro.adddata(data)
    cerebro.addstrategy(TestStg)
    cerebro.addobserver(bt.observers.DrawDown)
    cerebro.addobserver(bt.observers.TimeReturn)
    cerebro.addanalyzer(btanalyzers.DrawDown, _name='mydrawdown')
    cerebro.addanalyzer(btanalyzers.Returns, _name='myreturn')
    cerebro.addanalyzer(btanalyzers.TradeAnalyzer, _name='tdanalyzer')
    cerebro.addanalyzer(btanalyzers.TimeReturn, _name='TimeReturn')
    cerebro.addanalyzer(bt.analyzers.SharpeRatio, _name='_SharpeRatio', timeframe=bt.TimeFrame.Days, annualize=True, riskfreerate=0) # 计算夏普比率
    cerebro.broker.setcash(100000.0)

    print('Starting Portfolio Value: %.2f' % cerebro.broker.getvalue())
    thestrat = cerebro.run()[0]
    dd = thestrat.analyzers.mydrawdown.get_analysis()
    rt = thestrat.analyzers.myreturn.get_analysis()
    td_an = thestrat.analyzers.tdanalyzer.get_analysis()
    pnl = pd.Series(thestrat.analyzers.TimeReturn.get_analysis())
    # 计算累计收益
    cumulative = (pnl + 1).cumprod()
    # 计算回撤序列
    max_return = cumulative.cummax()
    drawdown = (cumulative - max_return) / max_return
    statics = print_statics(dd, rt, td_an)
    print(thestrat.analyzers._SharpeRatio.get_analysis())
    print('Final Portfolio Value: %.2f' % cerebro.broker.getvalue())
    cerebro.plot(style='candle')


