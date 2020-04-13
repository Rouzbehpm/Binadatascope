#!/usr/bin/python3
import os
import gzip
import glob

Province = 'Markazi'

# Path to .gz files in all directory of the Province
IPDR_LogFiles_Path = '/home/bida/IPDR_Pipelines/IPDR_LogFiles/' + Province + '/**/*.gz'

# Path to non-Corrupted (Healthy LogFiles) .gz files coming from the Province
Healthy_LogFiles = '/home/bida/IPDR_Pipelines/Healthy_LogFiles/' + Province + '/'

# Path to Corrupted .gz files coming from the Province
Corrupted_LogFiles = '/home/bida/IPDR_Pipelines/Corrupted_LogFiles/' + Province + '/'

# Read all .gz file in the path of the Province
IPDR_LogFiles = [f for f in glob.glob(IPDR_LogFiles_Path, recursive=True)]

for f in IPDR_LogFiles:  # Get file loop
    try:
        is_corrupt = os.system('gzip -t {}'.format(f))  # Check that .gz file is Corrupted or not
        folder_name = f.split('/')[6]  # Create a folder name with name of the Date
        if is_corrupt == 0:
            Healthy_Path = Healthy_LogFiles + folder_name  # Create dir for healthy file
            if not os.path.exists(Healthy_Path):  # check folder exists or not exists
                os.makedirs(Healthy_Path)
            os.system('mv {} {}'.format(f, Healthy_Path))  # Move healthy file to destination
        else:
            Corrupted_Path = Corrupted_LogFiles + folder_name  # Create dir for corrupt file
            if not os.path.exists(Corrupted_Path):  # check folder  exists or not exists
                os.makedirs(Corrupted_Path)
            os.system('mv {} {}'.format(f, Corrupted_Path))  # Move corrupt file to destination
    except FileNotFoundError:  # Exeption Handling
        print("Error")
