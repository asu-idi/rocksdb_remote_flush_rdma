
#!/usr/bin/python3


import os
import subprocess


import os

for i in range(3, 4096):
    os.system("ipcrm -m {}".format(i))