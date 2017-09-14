import pickle


def load(path):
    f = open(path, "rb")
    dicts = pickle.load(f)
    f.close()
    return dicts


def dump(dicts, path):
    f = open(path, "wb")
    pickle.dump(dicts, f, protocol=pickle.HIGHEST_PROTOCOL)
    f.close()
