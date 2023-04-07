from pyrecordbase import pyrecordbase
import json
ins = pyrecordbase.Connect("tls://127.0.0.1:8500", "$RECORDBASE_AUTH", 100)
print(ins)

entry = { "tenant": "jet", "primary_key": "alex" }
bin = json.dump(entry)
print(bin)
ins.Merge(bin, 100)
print("Merged")
entry = ins.Get("jet", "alex", 100)
print(entry)
