import os
import sys
import gzip
import glob
import datetime
import time
import csv


def validate(date_text):
    global request_date
    try:
        datetime.datetime.strptime(date_text, '%Y%m%d')
        request_date = date_text
    except ValueError:
        today = datetime.datetime.today()
        request_date = datetime.datetime.strftime(today, '%Y%m%d')
       

#Get Today in YYYYMMDD format
request_date = input('Enter Date (YYYYMMDD) or enter anything for today:\n')
validate(request_date)
#print(request_date)
#sys.argv = ['','-ilam']

#Start extracting healthy and corrupted files
if len (sys.argv) > 1:
    for arg in range (1,len(sys.argv)):
        total_count = 0
        healthy_count = 0
        corrupt_count = 0
        province = sys.argv[arg][1:].lower()
        #print ('Province {} start'.format(province))
        path = 'F:/BinaDataScope/IPDR/Logs/IPDR_Pipelines/IPDR_Logfiles/' + province + '/' + request_date
        # Path to .gz files in all directory of the Province
        IPDR_LogFiles_Path = path + '/**/*.gz'
        # Path to non-Corrupted (Healthy LogFiles) .gz files coming from the Province
        Healthy_LogFiles = 'F:/BinaDataScope/IPDR/Logs/IPDR_Pipelines/Healthy_LogFiles/' + province + '/' 
        # Path to Corrupted .gz files coming from the Province
        Corrupted_LogFiles = 'F:/BinaDataScope/IPDR/Logs/IPDR_Pipelines/Corrupted_LogFiles/' + province + '/'
        #list of all of the compressed files with full path
        IPDR_LogFiles = [f for f in glob.glob(IPDR_LogFiles_Path, recursive=True)]
        total_count += len(IPDR_LogFiles)
        #StartUnzipProcess
        for f in IPDR_LogFiles:
            try:
                #print('{} start'.format(f))
                is_corrupt = os.system('gzip -t {}'.format(f))  # Check that .gz file is Corrupted or not
                folder_name = f.replace('\\','/').split('/')[7]  # Create a folder name with name of the Date
                if is_corrupt == 0:
                    Healthy_Path = Healthy_LogFiles + folder_name  # Create dir for healthy file
                    if not os.path.exists(Healthy_Path):  # check folder exists or not exists
                        os.makedirs(Healthy_Path)
                    os.system('cp {} {}'.format(f, Healthy_Path))  # Move healthy file to destination
                    #print('{} is healthy and moved'.format(f))
                    healthy_count += 1
                else:
                    Corrupted_Path = Corrupted_LogFiles + folder_name  # Create dir for corrupt file
                    if not os.path.exists(Corrupted_Path):  # check folder  exists or not exists
                        os.makedirs(Corrupted_Path)
                    os.system('cp {} {}'.format(f, Corrupted_Path))  # Move corrupt file to destination
                    #print('{} is NOT healthy and moved'.format(f))
                    corrupt_count += 1
            except FileNotFoundError:  # Exeption Handling
                print("Error")
        
        #show report
        print('Province: {}, Date: {}, Total files:{}, Healthy Files:{}, Corrupted Files:{}'.format(province, request_date,total_count,healthy_count, corrupt_count))
        
        #export report to csv
        file_exists = os.path.isfile('log_file.csv') #check if file exists
        with open('log_file.csv', mode='a') as log_file:
            field_names = ['TimeStamp', 'Province', 'DateofFile','TotalFiles','HealthyFiles','CorruptedFiles']
            log_writer = csv.DictWriter(log_file, delimiter=',', fieldnames = field_names)
            if not file_exists:
                log_writer.writeheader() #write the header if file does not exist
            log_writer.writerow({'TimeStamp':datetime.datetime.strftime(datetime.datetime.now(), '%Y-%m-%d %H:%M:%S'), 'Province': province, 'DateofFile': request_date, 'TotalFiles': total_count,'HealthyFiles': healthy_count,'CorruptedFiles': corrupt_count })
            
