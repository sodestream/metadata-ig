# List of columns that will be generated (one csv file for each mailing list):

# sender/receiver email - email address of the person sending/receiving the email (sender is sending a message that is replying to something previously sent by the receiver)
# message_id, uid, uid_validity - these fields uniquely identify the current message and can be used to look it up in the mailarchive
# reply_to_message_id, reply_to_uid, reply_to_uid_validity - identify the message that is being replied to
# timestamp - time of the message
# maling_list_name - name of the mailing list 

# Things that were in the previous version but slightly different
# There is no PersonID, only email, but it will be easy to map email -> person_id, when we decide how we generate the person_ids and where we keep them
# unique message identifiers (message_id, uid, and uid_validity) are included for the current and the replied to message 
# - these can be used to look up the message in the mailarchive if extra metadata about them is needed

# Things missing wrt previous version:
# type: "reply_to" or "reply_self" is no longer there (we can bring this back if needed, not sure)
# mailing list type (at the group, year, month level) - e.g., "wg", "meeting", or "iab" ... is also omitted, if it is needed you need to pull this straight from the DT 
# - there is some code from Prashant somewhere that does exactly this but atm is not included here
# "time since first email", and "max time" for both sender and recipient, are ommited, can be brought back if relevant 

# Notes:
# spam is *not excluded*, but can be filtered by looking up the message in the mailarchive and getting it's metadata (see metadata-spam in the documentation)
# automated mailing lists are also *not excluded* but can be filtered by ignoring the lists in the file automated_emails.txt (there is probably code somewhere that generates that too)


from ietfdata.mailarchive2 import *
import traceback
import re


TEST_MODE = False

def header_message_id(e):
    h = e.header("message-id")
    if len(h) > 0:
        return h[0]
    else:
        return None

# version of iterate over thread that explicitly returns all edges it traversed
def iterate_over_thread_E(node_pair: (Envelope, Envelope)) -> list[Envelope]:
    visited_ids, results, queue = set(), [], [node_pair]
    
    while len(queue) > 0:       
        current_node, parent_node = queue.pop(0)
        
        curr_id = header_message_id(current_node) 
        if curr_id in visited_ids or curr_id is None: # curr_id == None seems to make the db replies() request below hang
            continue

        results.append((current_node, parent_node))
        visited_ids.add(curr_id)
        
        direct_children = current_node.replies()
        for child in direct_children:
            queue.append((child, current_node))

    return results
        
ren = r'(?:\.?)([\w\-_+#~!$&\'\.]+(?<!\.)(@|[ ]?\(?[ ]?(at|AT)[ ]?\)?[ ]?)(?<!\.)[\w]+[\w\-\.]*\.[a-zA-Z-]{2,3})(?:[^\w])'
ren2 = r'([a-zA-Z0-9._-]+@[a-zA-Z0-9._-]+\.[a-zA-Z0-9_-]+)'
def extract_addr(e):        
    e = e.replace("'", "__apostrophe__")
    x = re.findall(ren, str(e))
    email = ""
    if len(x) == 0:
        x = re.findall(ren2, str(e))
        if len(x) > 0:
            email = x[0]
    else:
        email = x[0][0]

    email = email.replace("__apostrophe__", "'").lower()
    return email


def process_email_header(h):
    addrs = []
    if len(h) == 0:
        return ""
    for h_elem in h[0].split(","): # the lib usually returns a single entry with comma separated name/emails (but it might rarely return more than a single entry, which is in this code ignored)
        addrs.append(extract_addr(h_elem))
    return ",".join(addrs)

def make_intgraph(ma, outfilename):
    data = []
    for ml_name in ma.mailing_list_names():
        print("Working on list:" + ml_name)
        if TEST_MODE:
            if ml_name != "ietf-languages":
                continue 

        ml = ma.mailing_list(ml_name)
        
        total, no = 0, 0
        thr_root_dict = ml.threads(this_list_only = True)
        for thr_root_key in thr_root_dict:
            thr_root = thr_root_dict[thr_root_key][0]
            for (msg, parent) in iterate_over_thread_E((thr_root, None)):
                total+=1 
                mid = header_message_id(msg)
                if parent is not None:
                    parent_mid = header_message_id(parent)
                else:
                    continue # this will be the case only for the first message in the thread
                data.append((
                          process_email_header(msg.header("from")),                          
                          process_email_header(msg.header("to")),
                          process_email_header(parent.header("from")),
                          mid,
                          msg.uid(),
                          msg.uidvalidity(),
                          parent_mid,
                          parent.uid(),
                          parent.uidvalidity(),
                          msg.date(),
                          ml_name
                        ))
                 

        print("Finished, total msgs: " + str(total))
    out_df = pd.DataFrame(data,    columns = ["current_from_email",
                                              "current_to_email",
                                              "reply_to_from_email", 
                                              "current_message_id",
                                              "current_uid",
                                              "current_uidvalidity",
                                              "reply_to_message_id", 
                                              "reply_to_uid",
                                              "reply_to_uidvalidity",
                                              "date",
                                              "mailing_list_name"])
    #print(out_df.head())
    out_df.to_csv(outfilename, index = False)

    
