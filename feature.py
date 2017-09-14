import utils
import datetime
# import gensim
from textblob import TextBlob

meta = utils.load("dicts/meta.p")
user = utils.load("dicts/user.p")
store = utils.load("dicts/store.p")
reviews = utils.load("dicts/reviews.p")
store_review = utils.load("dicts/store_review.p")
store_user = utils.load("dicts/store_user.p")

# model = gensim.models.KeyedVectors.load_word2vec_format('word2vec/GoogleNews-vectors-negative300.bin', binary=True)

def get_city(b_id):
    return store[b_id]["city"]

def get_state(b_id):
    stateVec = [0]*meta["cate_cols"]["state"]["num"]
    state = store[b_id]["state"]
    if state in meta["cate_cols"]["state"]["map"]:
        stateVec[meta["cate_cols"]["state"]["map"][state]] = 1
    else:
        stateVec[-1] = 1
    return stateVec

def get_stars(b_id):
    return store[b_id]["stars"]

def get_popularity(b_id):
    return len(store_review[b_id])

def get_name_size(b_id):
    return len(store[b_id]["name"].split())

def get_PosNeg_score(b_id):
    pos = 0.0
    neg = 0.0
    pos_len = 0.0
    neg_len = 0.0

    for review_id in store_review[b_id]:
        if reviews[review_id]["pol"] >= 0:
            pos += reviews[review_id]["pol"]
            pos_len += 1
        else:
            neg += reviews[review_id]["pol"]
            neg_len += 1

    if pos_len:
        pos /= pos_len
    if neg_len:
        neg /= neg_len

    return [pos, neg]

# def get_clarity(b_id):

#     tmp_sum = 0.0
#     count = 0.0

#     if not store[b_id]['categories']:
#         return [0.5, 1]
#     else:
#         for each in store[b_id]['categories']:
#             try:
#                 tmp_sum += model.similarity(each, store[b_id]['name'])
#                 count += 1
#             except:
#                 pass
#         if count:
#             return [tmp_sum/count, 0]
#         else:
#             return [0.5, 1]

def get_name_polar(b_id):
    pattern = TextBlob(store[b_id]["name"])
    return pattern.sentiment[0]

def get_y(b_id, observe_t = 12):
    life_time = store[b_id]["end_t"] - store[b_id]["start_t"]
    longer = life_time.days/30 - observe_t
    if longer > 0:
        return store[b_id]["is_open"]
    else:
        if store[b_id]["is_open"]:
            return -1
        else:
            return 0

def get_elite_cnt(b_id, observe_t=12):
    cnt = 0
    for user_id in store_user[b_id]:
        if user[user_id]["elite"][0] == "None":
            continue
        for yr in user[user_id]["elite"]:
            eli_date = datetime.datetime(int(yr), 1, 1)
            diff = eli_date - store[b_id]["start_t"]
            if diff.days/30 < observe_t:
                cnt = cnt + 1
                break
    return cnt
