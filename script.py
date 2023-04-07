from pyrecordbase import pyrecordbase
ins = pyrecordbase.Connect("127.0.0.1:8500", "env:RECORDBASE_AUTH", True, 100)
print(ins)
entry = ins.Get("jet", "alex", 100)
print(entry)
