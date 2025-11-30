import numpy as np
from tokenizer import CustomTokenizer
from tqdm import tqdm

tokenizer = CustomTokenizer()



in_path = ".txt"
out_file = ".bin"




BATCH_LINES = 20000           
total_tokens = 0
batch = []

lines=0
with open(in_path, "r", encoding="utf-8") as f, open(out_file, "wb") as out:

    for line in tqdm(f, desc="Lines", unit="lines"):
        lines+=1
        batch.append(line)
        if len(batch) >= BATCH_LINES:
            enc = tokenizer.encode(batch)   
            for ids in enc:
                total_tokens += len(ids)
                if len(ids) > 0:
                    np.array(ids, dtype=np.int32).tofile(out)
            batch = []
        


    if batch:
        enc = tokenizer.encode(batch) 
        for ids in enc:
            total_tokens += len(ids)
            if len(ids) > 0:
                np.array(ids, dtype=np.int32).tofile(out)


print(f"Number of lines: {lines}")
print(f"Number of tokens: {total_tokens}")



