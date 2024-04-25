from preprocess_intgraph import *
import json


jdata = json.load(open("config.json","r"))

ma = MailArchive(mongodb_username = jdata["mdb_uname"], mongodb_password = jdata["mdb_pass"])
make_intgraph(ma, "./intgraph.csv")


