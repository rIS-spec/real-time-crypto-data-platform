with open(".env.cloud") as f:
    line = f.read()

pw = line.split("neondb_owner:")[1].split("@")[0]
print("length:", len(pw))
print("first char:", repr(pw[0]))
print("last char:", repr(pw[-1]))