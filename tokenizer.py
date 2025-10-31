
from typing import List, Dict



class CustomTokenizer:
    def __init__(self):

        specials = ["<", ">", ",", "\n", ":", "null","pad",".","a","b","c","d","e","f"]
        


        neg_nums = [f"neg{i}" for i in range(1, 101)]  


        nums_3 = [str(i) for i in range(100, 1000)]
        nums_2 = [str(i) for i in range(10, 100)]
        nums_1 = [str(i) for i in range(0, 10)]


        letters = [a + b for a in "abcdef" for b in "abcdef"]


        self.tokens = []
        
        self.tokens.extend(neg_nums)
        self.tokens.extend(nums_3)
        self.tokens.extend(nums_2)
        self.tokens.extend(nums_1)
        self.tokens.extend(letters)
        self.tokens.extend(specials)

        
        self.unk_token = "null"
        self.tokens.append(self.unk_token)

        
        self.tok2id: Dict[str, int] = {t: i for i, t in enumerate(self.tokens)}
        self.id2tok: Dict[int, str] = {i: t for t, i in self.tok2id.items()}


        self.max_tok_len = max(len(t) for t in self.tokens if t != self.unk_token)

    def encode(self, text: List[str]) -> List[int]:

        out=[]
        for element in text:
            i = 0
            buffer = []
            L = len(element)
            while i < L:
                matched = False

                max_check = min(self.max_tok_len, L - i)
                for l in range(max_check, 0, -1):
                    piece = element[i:i+l]
                    if piece in self.tok2id:
                        buffer.append(self.tok2id[piece])
                        i += l
                        matched = True
                        break
                if not matched:
      
                    buffer.append(self.tok2id[self.unk_token])
                    i += 1
            out.append(buffer)
        return out

    def decode(self, ids: List[int]) -> str:
 
        parts = []
        for idx in ids:
            tok = self.id2tok.get(idx, self.unk_token)
            parts.append(tok)
        return "".join(parts)

    def vocab_size(self) -> int:
        return len(self.tokens)




