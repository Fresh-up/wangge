import pandas as pd
import datetime
import time
import math
import matplotlib.pyplot as plt
from tqdm.rich import tqdm

def to_unix_bn(str_t):
    dt = datetime.datetime.strptime(str_t, '%Y-%m-%d %H:%M:%S') + datetime.timedelta(hours=8)
    unix_time_ms = int(time.mktime(dt.timetuple()) * 1000 + dt.microsecond / 1000)
    return unix_time_ms

# 把逐笔数据合成5分钟k线
def to_kbar(from_time, to_time, path):
    
    from_time_ts = to_unix_bn(from_time)
    to_time_ts = to_unix_bn(to_time)
    data = pd.read_csv(path)

    data_list = []

    # 300000ms即5min
    for lower_bound in tqdm(range(from_time_ts, to_time_ts, 300000)):
        upper_bound = lower_bound + 300000
        
        segmented_data = data[(data['T'] >= lower_bound) & (data['T'] < upper_bound)]
        if not segmented_data.empty:
            data_list.append(segmented_data)
        else:
            print(lower_bound)
    

    kbar = []
    for it in data_list:
        lst = []
        lst.append(it['p'].iloc[0])
        lst.append(it['p'].max())
        lst.append(it['p'].min())
        lst.append(it['p'].iloc[-1])
        lst.append(it['q'].sum())
        bid = ((it['m'] == True) * it['q']).sum() # bid
        lst.append(bid)
        lst.append(it['q'].sum() - bid)
        lst.append(it['q'].sum() - 2 * bid) # delta
        lst.append(it.groupby('p')['q'].sum().idxmax()) # poc
        lst.append(it['T'].iloc[-1])
        kbar.append(lst)

    cols = ['open', 'high', 'low', 'close', 'vol', 'bid', 'ask', 'delta', 'poc', 'ts']
    kbar_df = pd.DataFrame(kbar, columns=cols)


    return kbar_df

# 把5min内的逐笔数据合成volume profile
# px_intv:价格区间，一般为1
# beishu:乘上币的价格变成整数，方便操作
# return: dataframe(两列：一列是px, 另一列：sz)
def vp(df, start_ts, end_ts, px_intv, beishu):
    
    data = df[(df['T'] >= start_ts) & (df['T'] < end_ts)]
    if data.empty:
        min_int = 0
        max_int = 0
    else:
        min_int = math.floor(min(data['p'])*beishu / px_intv) * px_intv
        max_int = math.ceil(max(data['p'])*beishu / px_intv) * px_intv
    col = ['px', 'sz']
    vp = []
    for px in range(min_int, max_int, px_intv):
        df1 = data[(data['p']*beishu >= px) & (data['p']*beishu < px + px_intv)]
        sz = round(df1['q'].sum(), 1)
        vp.append([px/beishu, sz])
    vp_df = pd.DataFrame(vp, columns=col)
    return vp_df

if __name__ in '__main__':

    pth = 'WLDUSDT0310~0413.csv'
    save_pth = 'WLDUSDTkbar0310~0413.csv'
    start_t = '2024-03-10 00:00:00'
    end_t = '2024-04-14 00:00:00'

    df = pd.read_csv(pth)

    kbar_df = to_kbar(start_t, end_t, pth)

    start_ts = to_unix_bn(start_t)
    end_ts = to_unix_bn(end_t)

    vol_pro = []
    for ts in tqdm(range(start_ts, end_ts, 300000)):
        vp_dff = vp(df, ts, ts + 300000, 1, 1000)
        vol_pro.append(vp_dff)


    poc_np = []
    for vp in vol_pro:
        if vp.empty:
            poc_np.append(0)
        else:
            poc_np.append(vp['px'][vp['sz'].idxmax()])
    kbar_df['poc'] = poc_np
    skewness_np = [x['sz'].skew() for x in vol_pro]
    kbar_df['skew'] = skewness_np

    # 下面三个目前没用
    kbar_df['skew_up_sig'] = 0
    kbar_df['skew_down_sig'] = 0
    kbar_df['openinterest'] = 0

    # 生成5分钟间隔的时间范围
    time_range = pd.date_range(start=start_t, end=end_t, freq='5T')

    time_series = pd.Series(time_range)
    time_series = time_series.iloc[:-1]
    kbar_df['ts'] = time_series

    datetime_str = [ts.strftime('%Y-%m-%d %H:%M:%S') for ts in kbar_df['ts']]

    kbar_df['ts'] = datetime_str # 字符串格式
    kbar_df[['Date', 'Time']] = kbar_df['ts'].str.split(' ', expand=True)

    kbar_df.to_csv(save_pth, index=False)