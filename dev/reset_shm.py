
#!/usr/bin/python3


import os
import subprocess


import os

for i in range(3, 201):
    os.system("ipcrm -m {}".format(i))