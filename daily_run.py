from Exchange import *
from OTC import *

start = datetime.now()

# main
receivers_email_list = ['']

gmail(receivers_email_list, OTC_deal_with_data()).send_gmail()

time_taken = datetime.now() - start
print("{} seconds taken...".format(round(time_taken.total_seconds(), 2)))