# %%
from email.parser import Parser
import os
from os.path import isfile, join
import pandas as pd
from pathlib import Path
path = Path("maildir")

for person in os.listdir(path):
    # At the person level
    # for debug just look at CEO
    # if person == 'lay-k':
    # export a CSV per person of all mailbox to keep things low in memory
    data_array = []
    print( '@person ', person )
    subdir_path = Path(f"{path}/{person}")

    # for resilience only drill-down on folders, not files
    if os.path.isdir(subdir_path):
        for mailbox in os.listdir(subdir_path):
            # print('mailbox', mailbox )
            # for debug just look at sent
            #if mailbox == 'sent'
            subsubdir_path = Path(f"{path}/{person}/{mailbox}")
            # for resilience only drill-down on folders, not files
            if os.path.isdir(subsubdir_path):
                for email in os.listdir(subsubdir_path):
                    email_path = Path(f"{path}/{person}/{mailbox}/{email}")
                    # print(email_path)
                    # open the text file, for resilience only load files
                    if os.path.isfile(email_path):
                        with open(email_path, "r", encoding="ISO-8859-1") as f:
                            file_data = f.read()
                        # parse the text file
                        email = Parser().parsestr(file_data)
                        email_record = {
                            'id':email['message-id']
                            , 'date':email['Date']
                            , 'From':email['From']
                            , 'To':email['To']
                            , 'Subject':email['Subject']
                            , 'mailbox': mailbox
                            , 'person': person
                        }
                        data_array.append( email_record )
    df = pd.DataFrame(data_array)

    output_path = f"{Path('.//export')}/{person}.csv"
    print(f"{person} emails outputted to {output_path}")
    df.to_csv(output_path , index=False )
