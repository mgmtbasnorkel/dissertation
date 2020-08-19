from OTXv2 import OTXv2, IndicatorTypes #AlienVault's script files  - https://github.com/AlienVault-OTX/OTX-Python-SDK
import pandas as pd
import time #calculate fetch time
import os #extract file size
import gc

#set job version
ver = 'AlienVault'

start_time = time.time() #calc fetch time

#collection
otx = OTXv2("#YOUR_UNIQUE_API_KEY#") ##unique api key
pulses = otx.getall()

fetch_time = round(time.time() - start_time, 2)
print('Fetch Succesful')

#pre-processing
pulses_df = pd.json_normalize(pulses)
#pulses_df['created'] = pd.to_datetime(pulses_df.created)
#pulses_df['modified'] = pd.to_datetime(pulses_df.modified)

#Save
#Pulses
#pulses_df.to_csv('./fetched_data/pulses_set_' + ver + '.csv') #csv (deprecated)
pulses_df.to_pickle('./fetched_data/' + ver + '_pulses_set' + '.pkl') 

#remove reduntant columns
pulses_df = pulses_df.loc[:,['indicators', 'name', 'id']]
gc.collect()

#Indicators
full_iocs_export = pd.DataFrame(columns=['indicator', 'description', 'title', 'created', 'is_active', 'content',
          'expiration', 'type', 'id', 'pulse_name', 'pulse_id'])

for uid in range(0, pulses_df.shape[0]):
    uid_pulses = pulses_df['indicators'][uid]
    uid_pulses = pd.json_normalize(uid_pulses)
    uid_pulses['pulse_name'] = pulses_df['name'][uid]
    uid_pulses['pulse_id'] = pulses_df['id'][uid]
    full_iocs_export = full_iocs_export.append(uid_pulses)
    
#full_iocs_export.to_csv('/project/OTX-Python-SDK/fetched_data/full_iocs_set_' + ver + '.csv') #(deprecated)
pulses_df.to_pickle('./fetched_data/' + ver +'_full_iocs_set' +  '.pkl')

#calculate file size
suffixes = ['B', 'KB', 'MB', 'GB', 'TB', 'PB'] #https://stackoverflow.com/a/14996816/13030358 #convert to mb
def humansize(nbytes):
    i = 0
    while nbytes >= 1024 and i < len(suffixes)-1:
        nbytes /= 1024.
        i += 1
    f = ('%.2f' % nbytes).rstrip('0').rstrip('.')
    return '%s %s' % (f, suffixes[i])
    
#File size for Pulses:
#pulses_df_memory_usage = humansize(os.stat('./fetched_data/pulses_set_' + ver + '.csv').st_size )#deprecated
pulses_df_memory_usage = humansize(os.stat('./fetched_data/' + ver + '_pulses_set' + '.pkl').st_size )
File size for indicators
full_iocs_export_memory_usage = humansize(os.stat('./fetched_data/full_iocs_set_' + ver + '.csv').st_size ) #deprecated
full_iocs_export_memory_usage = humansize(os.stat('./fetched_data/' + ver +'_full_iocs_set' +  '.pkl').st_size )

print('Completed!')

f= open("./fetched_data/running_results","a+")
for i in range(1):
    f.write("======== \n")
    f.write(("Job Version: " + ver  +" (pickle export)"+ "\n" ))
    # f.write("Total pulses: " + str(len(pulses)) + " with size " + pulses_df_memory_usage + "\n" )
    # f.write("Total IoCs: "+ str(full_iocs_export.shape[0]) + " with size " + full_iocs_export_memory_usage + "\n" )
    f.write("Total Fetch time: %s  seconds" % (fetch_time) + "\n" )
    f.write("======== \n")
f.close()