from pyrecordbase import pyrecordbase
ins = pyrecordbase.Connect("tls://127.0.0.1:8500", "$RECORDBASE_AUTH", 100)
print(ins)
e = pyrecordbase.Entry(Tenant="jet", PrimaryKey="alex")
e.Attributes = {"a": "bin"}
print(e)
ins.Merge(e, 100)
print("Merged")
entry = ins.Get("jet", "alex", 100)
print(entry.PrimaryKey)
