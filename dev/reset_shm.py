
#!/usr/bin/python3


import os
import subprocess
import sys

val = int(sys.argv[1])
val2 = int(sys.argv[2])

print(val,' ',val2)
for k in range(val-val2+11, val+1):
    try:
        subprocess.run(["ipcrm", "-m", str(k)], check=True)
    except subprocess.CalledProcessError:
        print(f"Command 'ipcrm -m {k}' failed, exiting loop.")
        # break