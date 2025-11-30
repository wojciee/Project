import pandas as pd


input_path="C:\\Users\\wojte\\Desktop\\"


input_filename="deauth"


file_att=input_path+input_filename


cols_before= ['frame.time_delta','frame.len','radiotap.length','radiotap.present.tsft','wlan_radio.data_rate','radiotap.channel.freq','radiotap.channel.flags.cck','radiotap.channel.flags.ofdm','wlan_radio.signal_dbm','wlan.fc.type','wlan.fc.type_subtype','wlan.fc.ds','wlan.fc.retry','wlan.fc.pwrmgt','wlan.fc.moredata','wlan.fc.protected','wlan.duration','wlan.fc.frag','wlan.seq']
cols= ['att_id','index_id','frame.time_delta_us','frame.len','radiotap.length','radiotap.present.tsft','wlan_radio.data_rate','radiotap.channel.freq','radiotap.channel.flags.cck','radiotap.channel.flags.ofdm','wlan_radio.signal_dbm','wlan.fc.type','wlan.fc.type_subtype','wlan.fc.ds','wlan.fc.retry','wlan.fc.pwrmgt','wlan.fc.moredata','wlan.fc.protected','wlan.duration','wlan.fc.frag','wlan.seq','wlan_mgt.fixed.auth_seq']


def clean_str_int(x):
    if x == "null" or pd.isna(x) or x == "" or x == "?":
        return "null"
    try:
        return str(int(x))
    except (ValueError, TypeError):
        return "null"

def clean_signal_dbm(x):
    if x == "null" or pd.isna(x) or x == "" or x == "?":
        return "null"
    try:
        return "neg" + str(int(-1 * float(x)))
    except (ValueError, TypeError):
        return "null"

def clean_type_subtype(x):
    if x == "null" or pd.isna(x) or x == "" or x == "?":
        return None
    try:
        return str(int(x, 0) & 0b1111)
    except (ValueError, TypeError):
        return None

def clean_ds(x):
    if x == "null" or pd.isna(x) or x == "" or x == "?":
        return "null"
    try:
        return str(int(x, 0))
    except (ValueError, TypeError):
        return "null"


first=True
for chunk in (pd.read_csv(file_att+".csv", chunksize=100000, low_memory=False,usecols=cols_before)):

    index_id=(((chunk.index % 100)-99)*-1).astype(str)
    chunk['att_id'] = "<0"
    chunk['index_id']=index_id.astype(str) + ">"
    chunk["frame.time_delta_us"] = pd.to_numeric(chunk["frame.time_delta"], errors="coerce") * 1e6
    chunk['wlan_mgt.fixed.auth_seq'] = 'null'

    chunk = chunk[cols]



    chunk['frame.time_delta_us']=chunk['frame.time_delta_us'].map(clean_str_int)
    chunk['frame.len']=chunk['frame.len'].map(clean_str_int)
    chunk['radiotap.length']=chunk['radiotap.length'].map(clean_str_int)
    chunk['radiotap.present.tsft']=chunk['radiotap.present.tsft'].map(clean_str_int)
    chunk['wlan_radio.data_rate']=chunk['wlan_radio.data_rate'].map(clean_str_int)
    chunk['radiotap.channel.freq']=chunk['radiotap.channel.freq'].map(clean_str_int)
    chunk['radiotap.channel.flags.cck']=chunk['radiotap.channel.flags.cck'].map(clean_str_int)
    chunk['radiotap.channel.flags.ofdm']=chunk['radiotap.channel.flags.ofdm'].map(clean_str_int)
    chunk['wlan_radio.signal_dbm'] = chunk['wlan_radio.signal_dbm'].map(clean_signal_dbm)
    chunk['wlan.fc.type']=chunk['wlan.fc.type'].map(clean_str_int)
    chunk['wlan.fc.type_subtype'] = chunk['wlan.fc.type_subtype'].map(clean_type_subtype)
    chunk['wlan.fc.ds'] = chunk['wlan.fc.ds'].map(clean_ds)
    chunk['wlan.fc.retry']=chunk['wlan.fc.retry'].map(clean_str_int)
    chunk['wlan.fc.pwrmgt']=chunk['wlan.fc.pwrmgt'].map(clean_str_int)
    chunk['wlan.fc.moredata']=chunk['wlan.fc.moredata'].map(clean_str_int)
    chunk['wlan.fc.protected']=chunk['wlan.fc.protected'].map(clean_str_int)
    chunk['wlan.duration']=chunk['wlan.duration'].map(clean_str_int)
    chunk['wlan.fc.frag']=chunk['wlan.fc.frag'].map(clean_str_int)
    chunk['wlan.seq'] = chunk['wlan.seq'].map(clean_str_int)
    chunk['wlan_mgt.fixed.auth_seq']=chunk['wlan_mgt.fixed.auth_seq'].map(clean_str_int)
    
    chunk['wlan_rsna_eapol.keydes.msgnr']="null"

    chunk.to_csv(file_att+".txt", mode="w" if first else "a", index=False, header=False)
    first = False