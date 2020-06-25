################################################################################
#
# Module: doc-list.py
#
################################################################################

import azure.cosmos.documents as documents
import azure.cosmos.cosmos_client as cosmos_client
import azure.cosmos.exceptions as exceptions
import datetime
import math
import pickle
import sys
import os

from collections import defaultdict

from complete_pipeline_run import *

################################################################################
# Helper class
################################################################################

class CosmosDBGuard(cosmos_client.CosmosClient):
    def __init__(self, obj):
        self.obj = obj

    def __enter__(self):
        return self.obj

    def __exit__(self, exception_type, exception_val, trace):
        # extra cleanup in here
        self.obj = None

def get_last_date():
    data_location = os.path.join("/Users/jashoo/data/")
    pipeline_runs = pickle.load(open(os.path.join(data_location, "pipelines.txt"), "rb" ))

    pipeline_runs.sort(key=lambda item: item["DateStart"])
    last_run = pipeline_runs[-1]

    return last_run["DateStart"]

################################################################################
################################################################################

def add_calculated_data_to_db(db, min_date):
    pipeline_container = db.get_container_client("runtime-pipelines")
    pipeline_runs = list(pipeline_container.read_all_items(max_item_count=1000))

    pipeline_runs = [item for item in pipeline_runs if datetime.datetime.strptime(item["DateStart"], "%Y-%m-%dT%H:%M:%S") > min_date]

    pipeline_runs.sort(key=lambda item: item["DateStart"])
    pipeline_runs.reverse()

    complete_pipelines = []
    failed_pipelines = []

    for index, pipeline in enumerate(pipeline_runs):
        item_number = index + 1
        complete_pipeline = CompletePipelineRun(db, pipeline, verbose=False)
        complete_pipeline.update_pipeline_if_needed()
        print("[{}:{}]: {}: Setup time: {}. Libraries setup time: {}, CoreCLR setup time: {}, Mono setup time: {}, Run time: {}. Total tests: {}. Failed tests: {}.".format(item_number, len(pipeline_runs), pipeline["id"], complete_pipeline.total_setup_time, complete_pipeline.total_libraries_setup_time, complete_pipeline.total_coreclr_setup_time , complete_pipeline.total_mono_setup_time, complete_pipeline.total_run_time, complete_pipeline.total_tests_run, complete_pipeline.total_test_failures))
        
        if not complete_pipeline.passed():
            failed_pipelines.append(complete_pipeline)

        complete_pipelines.append(complete_pipeline)

    return complete_pipelines, failed_pipelines

def analyze(failed_pipelines, only_ci=False):
    """ Go through failing runs and give failure reasons
    """

    failure_types = defaultdict(lambda: [])
    timeouts_by_queue = defaultdict(lambda: 0)

    def add_to_timeout_queue(queue):
        if "@" in queue:
            queue = queue.split("@")[0]
            queue = queue.split(")")[1]

        timeouts_by_queue[queue.lower()] += 1

    for complete_pipeline in failed_pipelines:
        if only_ci is True and complete_pipeline.pipeline["BuildReasonString"] != "BatchedCI":
            continue
        
        failure_reasons = complete_pipeline.analyze()

        for failure_reason in failure_reasons:
            submissions = []
            if failure_reason == "Helix Submission Timeout":
                for failure in failure_reasons[failure_reason]:
                    submissions += failure[2]

                for submission in submissions:
                    queues = submission["Queues"]
                    for item in queues:
                        add_to_timeout_queue(item)

            failure_types[failure_reason].append(complete_pipeline)

    for failure in failure_types:
        print("[{}]: ({})".format(failure, len(failure_types[failure])))
        print("-----------------------------------------------------------")

        for runs in failure_types[failure]:
            print("[{}]: {}".format(runs.pipeline["id"], runs.pipeline["BuildUri"]))
            
        print("-----------------------------------------------------------")

    for queue in timeouts_by_queue:
        print("[{}]: {}".format(queue, timeouts_by_queue[queue]))

def analyze_submisions(complete_pipelines, queue):
    submissions = []
    for index, complete_pipeline in enumerate(complete_pipelines):
        submissions += complete_pipeline.analyze_submissions()

    submissions_by_job_name = defaultdict(lambda: [])

    for submission in submissions:
        submissions_by_job_name[submission["JobName"]].append(submission)

    print("Submissions:")
    print("-------------------------------------------------------------------")

    for job in submissions_by_job_name:
        total_setup_time = 0
        total_run_time = 0

        for submission in submissions_by_job_name[job]:
            total_setup_time += submission["TotalSetupTime"]
            total_run_time += submission["TotalRunTime"]

        average_setup_time = total_setup_time / len(submissions_by_job_name[job])
        average_run_time = total_run_time / len(submissions_by_job_name[job])

        print("[{}]: {} Submissions, Setup Time {}, Run Time {}".format(job, len(submissions_by_job_name[job]), average_setup_time, average_run_time))

def main():
    with CosmosDBGuard(cosmos_client.CosmosClient("https://coreclr-infra.documents.azure.com:443/", {'masterKey': os.environ["coreclrInfraKey"]} )) as client:
        try:
            #get_last_runtime_pipeline(client)
            #update_pickled_data(client)
            #bucket_from_disk()
            #compare_run("20200526.78", "20200526.76")

            #get_all_pipelines_run_for_user("jashook", "Submit to helix without managed pdbs")
            db = client.get_database_client("coreclr-infra")

            # Only look back one week
            seven_days_ago = datetime.datetime.now() - datetime.timedelta(days=7)
            complete_pipelines, failed_pipelines = add_calculated_data_to_db(db, seven_days_ago)

            analyze(failed_pipelines)

            print ("----------------------------------------------------------")

            analyze_submisions(complete_pipelines, "ubuntu.1804.armarch.open")

        except exceptions.CosmosHttpResponseError as e:
            print('Error. {0}'.format(e))

        print()

################################################################################
################################################################################

if __name__ == "__main__":
    main()