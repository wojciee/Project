import pandas as pd


input_path="C:"
dataset="wpa3"
input_filename="Krack"


file_att=input_path+input_filename

include_mac=False

cols_before= ['Label','frame.time_delta','frame.len','radiotap.length','radiotap.present.tsft','wlan_radio.data_rate','radiotap.channel.freq','radiotap.channel.flags.cck','radiotap.channel.flags.ofdm','wlan_radio.signal_dbm','wlan.fc.type','wlan.fc.subtype','wlan.fc.ds','wlan.fc.retry','wlan.fc.pwrmgt','wlan.fc.moredata','wlan.fc.protected','wlan.duration','wlan.da','wlan.sa','wlan.fc.frag','wlan.seq','wlan_rsna_eapol.keydes.msgnr']
cols= ['att_id','index_id','frame.time_delta_us','frame.len','radiotap.length','radiotap.present.tsft','wlan_radio.data_rate','radiotap.channel.freq','radiotap.channel.flags.cck','radiotap.channel.flags.ofdm','wlan_radio.signal_dbm','wlan.fc.type','wlan.fc.subtype','wlan.fc.ds','wlan.fc.retry','wlan.fc.pwrmgt','wlan.fc.moredata','wlan.fc.protected','wlan.duration','wlan.da','wlan.sa','wlan.fc.frag','wlan.seq','wlan_mgt.fixed.auth_seq','wlan_rsna_eapol.keydes.msgnr']



    
attack_map_wpa3={
    'Normal':0,
    'Deauth':1,
    'Disas':2,
    '(Re)Assoc':3,
    'RogueAP':4,
    'Evil_Twin':5,
    'Krack':6,
    'Kr00K':7,
    'Kr00k':7,
    'amok':8,
    'cts':9,
    'rts':10,
    'probe_request':11,
    'probe_response':12,
    'Beacon Flood':13,
    'authentication_request':14,
    'power':15
}

attack_map_a3={
    'Normal':0,
    'Deauth':1,
    'Disas':2,
    '(Re)Assoc':3,
    'RogueAP':4,
    'Evil_Twin':5,
    'Krack':6,
    'Kr00K':7,
    'Kr00k':7,
    'amok':8,
    'cts':9,
    'rts':10,
    'probe_request':11,
    'probe_response':12,
    'Beacon':13,
    'authentication_request':14,
    'power':15
}



if dataset=="wpa3":
    attack_map=attack_map_wpa3
else:
    attack_map=attack_map_a3
    

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

def clean_hex(x):
    if x == "null" or pd.isna(x) or x == "" or x == "?":
        return "null"
    try:
        return str(int(x, 0))
    except (ValueError, TypeError):
        return "null"
    
def clean_attack_id(x):
    if x == "null" or pd.isna(x) or x == "" or x == "?":
        return "null"
    return f"<{int(x)}"

def clean_index_id(index):
    return str(((index % 100) - 99) * -1) + ">"


def format_attack_id(x):
    if pd.isna(x):
        return "null"
    return f"<{int(x)}"

def clean_mac(x):

    if x in ("?", "", "null") or pd.isna(x):
        return "null"
    try:
        return str(x).split('-')[0]
    except (ValueError, TypeError):
        return "null"
    
def clean_tsft(x):
    if pd.isna(x):
        return "null"
    x_str = str(x)
    if x_str == "0-0-0":
        return "0"
    elif x_str == "1-0-0":
        return "1"
    else:
        return "null"



if include_mac==False:
    to_remove = ['wlan.sa', 'wlan.da']
    cols_before = [col for col in cols_before if col not in to_remove]
    cols = [col for col in cols if col not in to_remove]



first=True
for chunk in (pd.read_csv(file_att+".csv", chunksize=100000, low_memory=False,usecols=cols_before)):


    chunk['att_id'] = chunk['Label'].map(attack_map).map(format_attack_id)
    chunk['index_id']=chunk.index.map(clean_index_id)
    chunk["frame.time_delta_us"] = pd.to_numeric(chunk["frame.time_delta"], errors="coerce") * 1e6
    chunk['wlan_mgt.fixed.auth_seq'] = 'null'

    chunk = chunk[cols]

    chunk['frame.time_delta_us']=chunk['frame.time_delta_us'].map(clean_str_int)
    chunk['frame.len']=chunk['frame.len'].map(clean_str_int)
    chunk['radiotap.length']=chunk['radiotap.length'].map(clean_str_int)
    if dataset=="wpa3":
        chunk['radiotap.present.tsft']=chunk['radiotap.present.tsft'].map(clean_tsft)
    else:
        chunk['radiotap.present.tsft']=chunk['radiotap.present.tsft'].map(clean_str_int)
    chunk['wlan_radio.data_rate']=chunk['wlan_radio.data_rate'].map(clean_str_int)
    chunk['radiotap.channel.freq']=chunk['radiotap.channel.freq'].map(clean_str_int)
    chunk['radiotap.channel.flags.cck']=chunk['radiotap.channel.flags.cck'].map(clean_str_int)
    chunk['radiotap.channel.flags.ofdm']=chunk['radiotap.channel.flags.ofdm'].map(clean_str_int)
    chunk['wlan_radio.signal_dbm'] = chunk['wlan_radio.signal_dbm'].map(clean_signal_dbm)
    chunk['wlan.fc.type']=chunk['wlan.fc.type'].map(clean_str_int)
    chunk['wlan.fc.subtype']=chunk['wlan.fc.subtype'].map(clean_str_int)
    chunk['wlan.fc.ds'] = chunk['wlan.fc.ds'].map(clean_hex)
    chunk['wlan.fc.retry']=chunk['wlan.fc.retry'].map(clean_str_int)
    chunk['wlan.fc.pwrmgt']=chunk['wlan.fc.pwrmgt'].map(clean_str_int)
    chunk['wlan.fc.moredata']=chunk['wlan.fc.moredata'].map(clean_str_int)
    chunk['wlan.fc.protected']=chunk['wlan.fc.protected'].map(clean_str_int)
    chunk['wlan.duration']=chunk['wlan.duration'].map(clean_str_int)
    chunk['wlan.fc.frag']=chunk['wlan.fc.frag'].map(clean_str_int)
    chunk['wlan.seq'] = chunk['wlan.seq'].map(clean_str_int)
    chunk['wlan_mgt.fixed.auth_seq']=chunk['wlan_mgt.fixed.auth_seq'].map(clean_hex)
    chunk['wlan_rsna_eapol.keydes.msgnr']=chunk['wlan_rsna_eapol.keydes.msgnr'].map(clean_str_int)


    if include_mac==True:
        chunk['wlan.sa'] = chunk['wlan.sa'].map(clean_mac)
        chunk['wlan.da'] = chunk['wlan.da'].map(clean_mac)


    chunk.to_csv(file_att+".txt", mode="w" if first else "a", index=False, header=False)
    first = False
    

