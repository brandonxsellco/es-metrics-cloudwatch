import requests
import json
import boto3
from datetime import datetime
from threading import Thread
from time import sleep
import logging

####################################################################################
    #  ENVIRONMENT VARIABLES (CONFIGURABLE??)#
####################################################################################

api_list = {
}

AWS_ACCESS_KEY_ID = ''
AWS_SECRET_ACCESS_KEY = ''

NAMESPACE = 'ES Custom Metrics'

####################################################################################
    #  AMAZON CLOUDWATCH CLIENT SETUP (REMOVE CREDENTIALS) & LOGGING SETUP #
####################################################################################


# Remove handlers so it prints to CloudWatch logs
root = logging.getLogger()
if root.handlers:
    for handler in root.handlers:
        root.removeHandler(handler)

logging.basicConfig(format='[%(asctime)s] [%(threadName)s] %(message)s',level=logging.INFO)

# Do not hard code credentials
client = boto3.client(
    'cloudwatch',
    # Hard coded strings as credentials, not recommended.
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    region_name='eu-west-1'
)

####################################################################################
    #  MAIN METRIC FECTH & ADD METHOD #
####################################################################################

def main():

    pod1 = "search-xs-ws-search-pod1"
    pod2 = "search-xs-ws-search-pod2"
    pod3 = "search-xs-ws-search-pod3"

    # Define threads for pod metric collection
    pod1_thread = Thread(target = run_threads_for_domain, args = [pod1], name= "pod1_thread")
    pod2_thread = Thread(target = run_threads_for_domain, args = [pod2], name= "pod2_thread")
    pod3_thread = Thread(target = run_threads_for_domain, args = [pod3], name= "pod3_thread")

    ################################################

    # Start each thread
    logging.info("Starting pod1_thread...")
    pod1_thread.start()

    logging.info("Starting pod2_thread...")
    pod2_thread.start()

    logging.info("Starting pod3_thread...")
    pod3_thread.start()

    ################################################

    # Join threads
    pod1_thread.join()
    logging.info("pod1_thread finished...")

    pod2_thread.join()
    logging.info("pod2_thread finished...")

    pod3_thread.join()
    logging.info("pod3_thread finished...")


def run_threads_for_domain(domain):

    # Define threads for each API call
    pending_tasks_thread = Thread(target = pending_tasks_api_thread, args = [domain], name= "pending_tasks_thread")
    current_tasks_thread = Thread(target = current_tasks_api_thread, args = [domain], name= "current_tasks_thread")
    node_stats_thread = Thread(target = node_stats_api_thread, args = [domain], name= "node_stats_thread")
    cluster_stats_thread = Thread(target = cluster_stats_api_thread, args = [domain], name= "cluster_stats_thread")

    ################################################

    # Start each thread
    logging.info("[" + domain + "] Starting pending_tasks_thread...")
    pending_tasks_thread.start()

    logging.info("[" + domain + "] Starting current_tasks_thread...")
    current_tasks_thread.start()

    logging.info("[" + domain + "] Starting node_stats_thread...")
    node_stats_thread.start()

    logging.info("[" + domain + "] Starting cluster_stats_thread...")
    cluster_stats_thread.start()

    ################################################

    # Join threads
    pending_tasks_thread.join()
    logging.info("[" + domain + "] pending_tasks_thread finished...")

    current_tasks_thread.join()
    logging.info("[" + domain + "] current_tasks_thread finished...")

    node_stats_thread.join()
    logging.info("[" + domain + "] node_stats_thread finished...")

    cluster_stats_thread.join()
    logging.info("[" + domain + "] cluster_stats_thread finished...")

####################################################################################
    #  MASTER THREADS - PER API #
####################################################################################

# Thread function to get all metrics from the PEDNING_TASKS_API
def pending_tasks_api_thread(domain):

    count = 0
    pending_tasks = 0

    while count < 60:
        # Get current data off ES API's
        data = requests.get(api_list[domain]["pending_tasks"]).json()

        pending_tasks += pending_tasks_count(data, domain)

        # Sleep for 1s
        sleep(1)
        count += 1

    # Adjust metrics after minute sample
    adjusted_pending_tasks = float(pending_tasks) / 60.0

    # Add final metrics from minute sample
    logging.info("[" + domain + "] Adding metric [PendingTasksCount] = " + str(adjusted_pending_tasks))
    add_metric('PendingTasksCount', adjusted_pending_tasks, 'Count', domain)

    return

# Thread function to get all metrics from the CURRENT_TASKS_API
def current_tasks_api_thread(domain):

    count = 0
    current_tasks = 0

    while count < 60:
        # Get current data off ES API's
        data = requests.get(api_list[domain]["current_tasks"]).json()

        current_tasks += current_tasks_count(data, domain)

        # Sleep for 1s
        sleep(1)
        count += 1

    # Adjust metrics after minute sample
    adjusted_current_tasks = float(current_tasks) / 60.0

    # Add final metrics from minute sample
    logging.info("[" + domain + "] Adding metric [CurrentTasksCount] = " + str(adjusted_current_tasks))
    add_metric('CurrentTasksCount', adjusted_current_tasks, 'Count', domain)

    return

# Thread function to get all metrics from the NODE_STATS_API
def node_stats_api_thread(domain):

    count = 0

    # Initialise internal variables

    active_threads = {
        "index" : 0.0,
        "bulk" : 0.0,
        "get" : 0.0,
        "search" : 0.0,
        "force_merge" : 0.0
    }

    queued_threads = {
        "index" : 0.0,
        "bulk" : 0.0,
        "get" : 0.0,
        "search" : 0.0,
        "force_merge" : 0.0
    }

    # young_gc_latency = 0.0
    # old_gc_latency = 0.0f

    while count < 60:
        # Get current data off ES API's
        data = requests.get(api_list[domain]["node_stats"]).json()

        # young_gc_latency += get_young_gc_latency(data, domain)
        # old_gc_latency += get_old_gc_latency(data, domain)

        active_threads = get_active_threadpool_stats(data, domain, active_threads)
        queued_threads = get_queued_threadpool_stats(data, domain, queued_threads)

        # Sleep for 0.2s since long processing
        sleep(0.2)
        count += 1

    # Adjust metrics after minute sample
    active_threads = adjust_threads_dict(active_threads)
    queued_threads = adjust_threads_dict(queued_threads)

    # adjusted_young_gc_latency = float(young_gc_latency) / 60.0
    # adjusted_old_gc_latency = float(old_gc_latency) / 60.0

    # Add metrics for active & queued index threads
    logging.info("[" + domain + "] Adding metric [ActiveIndexThreads] = " + str(active_threads["index"]))
    add_metric('ActiveIndexThreads', active_threads["index"], 'Count', domain)

    logging.info("[" + domain + "] Adding metric [QueuedIndexThreads] = " + str(queued_threads["index"]))
    add_metric('QueuedIndexThreads', queued_threads["index"], 'Count', domain)

    # Add metrics for active & queued bulk threads
    logging.info("[" + domain + "] Adding metric [ActiveBulkThreads] = " + str(active_threads["bulk"]))
    add_metric('ActiveBulkThreads', active_threads["bulk"], 'Count', domain)

    logging.info("[" + domain + "] Adding metric [QueuedBulkThreads] = " + str(queued_threads["bulk"]))
    add_metric('QueuedBulkThreads', queued_threads["bulk"], 'Count', domain)

    # Add metrics for active & queued get threads
    logging.info("[" + domain + "] Adding metric [ActiveGetThreads] = " + str(active_threads["get"]))
    add_metric('ActiveGetThreads', active_threads["get"], 'Count', domain)

    logging.info("[" + domain + "] Adding metric [QueuedGetThreads] = " + str(queued_threads["get"]))
    add_metric('QueuedGetThreads', queued_threads["get"], 'Count', domain)

    # Add metrics for active & queued sesarch threads
    logging.info("[" + domain + "] Adding metric [ActiveSearchThreads] = " + str(active_threads["search"]))
    add_metric('ActiveSearchThreads', active_threads["search"], 'Count', domain)

    logging.info("[" + domain + "] Adding metric [QueuedSearchThreads] = " + str(queued_threads["search"]))
    add_metric('QueuedSearchThreads', queued_threads["search"], 'Count', domain)

    # Add metrics for active & queued merge threads
    logging.info("[" + domain + "] Adding metric [ActiveMergeThreads] = " + str(active_threads["force_merge"]))
    add_metric('ActiveMergeThreads', active_threads["force_merge"], 'Count', domain)

    logging.info("[" + domain + "] Adding metric [QueuedMergeThreads] = " + str(queued_threads["force_merge"]))
    add_metric('QueuedMergeThreads', queued_threads["force_merge"], 'Count', domain)

    # logging.info("Adding metric [JVMAvgYoungGCLatency] = " + str(adjusted_young_gc_latency) + " ms")
    # add_metric('JVMAvgYoungGCLatency', adjusted_young_gc_latency, 'Milliseconds', domain)
    #
    # logging.info("Adding metric [JVMAvgOldGCLatency] = " + str(adjusted_old_gc_latency))
    # add_metric('JVMAvgOldGCLatency', adjusted_old_gc_latency, 'Milliseconds', domain)

    return

# Thread function to get all metrics from the INDEXING_TASKS_API
def cluster_stats_api_thread(domain):

    count = 0

    # Initialise internal variables
    indexing_count = 0.0
    delete_count = 0.0
    search_count = 0.0

    index_latency = 0.0
    delete_latency = 0.0
    search_latency = 0.0

    while count < 60:
        # Get current data off ES API's
        data = requests.get(api_list[domain]["cluster_stats"]).json()

        indexing_count += get_indexing_count(data, domain)
        delete_count += get_delete_count(data, domain)
        search_count += get_search_count(data, domain)

        index_latency += get_index_latency(data, domain)
        delete_latency += get_delete_latency(data, domain)
        search_latency += get_search_latency(data, domain)

        # Sleep for 0.5s since long processing
        sleep(0.5)
        count += 1

    # Adjust metrics after minute sample

    adjusted_indexing_count = float(indexing_count) / 60.0
    adjusted_delete_count = float(delete_count) / 60.0
    adjusted_search_count = float(search_count) / 60.0

    adjusted_index_latency = float(index_latency) / 60.0
    adjusted_delete_latency = float(delete_latency) / 60.0
    adjusted_search_latency = float(search_latency) / 60.0


    # Add final metrics from minute sample
    logging.info("[" + domain + "] Adding metric [CurrentIndexCount] = " + str(adjusted_indexing_count))
    add_metric('CurrentIndexCount', adjusted_indexing_count, 'Count', domain)

    logging.info("[" + domain + "] Adding metric [CurrentDeleteCount] = " + str(adjusted_delete_count))
    add_metric('CurrentDeleteCount', adjusted_delete_count, 'Count', domain)

    logging.info("[" + domain + "] Adding metric [CurrentSearchCount] = " + str(adjusted_search_count))
    add_metric('CurrentSearchCount', adjusted_search_count, 'Count', domain)


    logging.info("[" + domain + "] Adding metric [IndexLatency] = " + str(adjusted_index_latency) + " ms")
    add_metric('IndexLatency', adjusted_index_latency, 'Milliseconds', domain)

    logging.info("[" + domain + "] Adding metric [DeleteLatency] = " + str(adjusted_delete_latency) + " ms")
    add_metric('DeleteLatency', adjusted_delete_latency, 'Milliseconds', domain)

    logging.info("[" + domain + "] Adding metric [SearchLatency] = " + str(adjusted_search_latency) + " ms")
    add_metric('SearchLatency', adjusted_search_latency, 'Milliseconds', domain)

    return


####################################################################################
    #  THREADPOOL METRICS (Indexing, Bulk, Get, Search, Merging) #
####################################################################################

# Method to collect number of active threads performing different tasks
def get_active_threadpool_stats(data, domain, active_threads):

    # Iterate over all nodes calculating the total number of active indexing threads
    for node in data['nodes']:

        # Get active threads stats
        active_threads["index"] += float(data['nodes'][node]['thread_pool']['index']['active'])
        active_threads["bulk"] += float(data['nodes'][node]['thread_pool']['bulk']['active'])
        active_threads["get"] += float(data['nodes'][node]['thread_pool']['get']['active'])
        active_threads["search"] += float(data['nodes'][node]['thread_pool']['search']['active'])
        active_threads["force_merge"] += float(data['nodes'][node]['thread_pool']['force_merge']['active'])

    return active_threads

# Method to collect number of queued threads performing different tasks
def get_queued_threadpool_stats(data, domain, queued_threads):

    # Iterate over all nodes calculating the total number of queued threads
    for node in data['nodes']:

        # Get queued threads stats
        queued_threads["index"] += float(data['nodes'][node]['thread_pool']['index']['queue'])
        queued_threads["bulk"] += float(data['nodes'][node]['thread_pool']['bulk']['queue'])
        queued_threads["get"] += float(data['nodes'][node]['thread_pool']['get']['queue'])
        queued_threads["search"] += float(data['nodes'][node]['thread_pool']['search']['queue'])
        queued_threads["force_merge"] += float(data['nodes'][node]['thread_pool']['force_merge']['queue'])

    return queued_threads


####################################################################################
    #  JVM HEAP & GARBAGE COLLECTION METRICS #
####################################################################################

# Method to collect avg young gc latency (should be very low)
def get_young_gc_latency(data, domain):

    young_gc_collection_count = 0.0
    young_gc_collection_time = 0.0

    # Iterate over all nodes calculating the total young gc collections & time
    for node in data['nodes']:
        young_gc_collection_count += float(data['nodes'][node]['jvm']['gc']['collectors']['young']['collection_count'])
        young_gc_collection_time += float(data['nodes'][node]['jvm']['gc']['collectors']['young']['collection_time_in_millis'])

    avg_latency = young_gc_collection_count / young_gc_collection_time

    return avg_latency

# Method to collect avg old gc latency (should be higher than young yet still reasonably low)
def get_old_gc_latency(data, domain):

    old_gc_collection_count = 0.0
    old_gc_collection_time = 0.0

    # Iterate over all nodes calculating the total young gc collections & time
    for node in data['nodes']:
        old_gc_collection_count += float(data['nodes'][node]['jvm']['gc']['collectors']['old']['collection_count'])
        old_gc_collection_time += float(data['nodes'][node]['jvm']['gc']['collectors']['old']['collection_time_in_millis'])

    avg_latency = old_gc_collection_count / old_gc_collection_time
    return avg_latency

####################################################################################
    #  PENDING & CURRENT TASKS #
####################################################################################

# Method to put the count of current pending tasks into Cloudwatch
def pending_tasks_count(data, domain):

    return len(data['tasks'])

# Method to put the count of current running tasks into Cloudwatch
def current_tasks_count(data, domain):

    tasks_count = 0

    # Iterate over all nodes getting the current_task count for each
    for node in data['nodes']:
        tasks_count += len(data['nodes'][node]['tasks'])

    return tasks_count

####################################################################################
    #  CURRENT SEARCH, INDEX & DELETE COUNTS #
####################################################################################

# Method to put the count of current running search queries into Cloudwatch
def get_search_count(data, domain):

    return data['_all']['total']['search']['query_current']

# Method to put the count of current running indexing tasks into Cloudwatch
def get_indexing_count(data, domain):

    indexing_count = data['_all']['total']['indexing']['index_current']

    return indexing_count

# Method to put the count of current running delete tasks into Cloudwatch
def get_delete_count(data, domain):

    delete_count = data['_all']['total']['indexing']['delete_current']
    return delete_count

####################################################################################
    #  CURRENT SEARCH, INDEX & DELETE LATENCY #
####################################################################################

# Method to put the average search latency (index_total/index_time_total) in ms into Cloudwatch
def get_search_latency(data, domain):

    search_total = float(data['_all']['total']['search']['query_total'])
    search_time = float(data['_all']['total']['search']['query_time_in_millis'])

    latency = search_total / search_time
    latency = round(latency, 4)

    return latency

# Method to put the average index latency (index_total/index_time_total) in ms into Cloudwatch
def get_index_latency(data, domain):

    index_total = float(data['_all']['total']['indexing']['index_total'])
    index_time = float(data['_all']['total']['indexing']['index_time_in_millis'])

    latency = index_total / index_time
    latency = round(latency, 4)

    return latency

# Method to put the average delete latency (delete_total/delete_time_total) in ms into Cloudwatch
def get_delete_latency(data, domain):

    delete_total = float(data['_all']['total']['indexing']['delete_total'])
    delete_time = float(data['_all']['total']['indexing']['delete_time_in_millis'])

    latency = delete_total / delete_time
    latency = round(latency, 4)
    return latency

####################################################################################
    #  GENERAL / MISC METHODS #
####################################################################################

# Method to add a custom metric to AWS Cloudwatch
def add_metric(metric_name, metric_value, unit, domain):
        response = client.put_metric_data(
            Namespace= 'ES Custom Metrics',
            MetricData=[
                {
                    'MetricName': metric_name,
                    'Dimensions': [
                        {
                            'Name': 'Domain',
                            'Value': domain
                        },
                    ],
                    'Timestamp': datetime.now(),
                    'Value': metric_value,
                    'Unit': unit
                },
            ]
        )

        logging.info("[" + metric_name + "] " + str(response))
        return

# Method to adjust the metrics within a threadpool dict (based on 60 samples)
def adjust_threads_dict(dict):

    dict["index"] /= 60.0
    dict["bulk"] /= 60.0
    dict["get"] /= 60.0
    dict["search"] /= 60.0
    dict["force_merge"] /= 60.0

    return dict

# Method to validate the response from a custom metric addition
def validate_response(response):

    status_code = response['ResponseMetadata']['HTTPStatusCode']

    if status_code == 200:
        return True
    else:
        return False

if __name__ == "__main__":
    main()
