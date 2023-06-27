
#!/usr/bin/python3


import os
import subprocess


import os

for i in range(131299, 131300):
    os.system("ipcrm -m {}".format(i))