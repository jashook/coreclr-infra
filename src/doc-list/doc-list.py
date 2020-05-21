################################################################################
#
# Module: doc-list.py
#
################################################################################

import azure.cosmos.documents as documents
import azure.cosmos.cosmos_client as cosmos_client
import azure.cosmos.errors as errors
import datetime
import os
import math

from collections import defaultdict

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

################################################################################
################################################################################

def read_helix_workitems_for_pipeline(client, pipeline_id=None):
    runtime_collection_link = "dbs/coreclr-infra/colls/runtime-pipelines"
    helix_collection_link = "dbs/coreclr-infra/colls/helix-jobs"

    pipeline_id = "20200505.36"

    if pipeline_id is None:
        runtime_docs = list(client.ReadItems(runtime_collection_link, {'maxItemCount': 1000}))
        last_pipeline = runtime_docs[-1]

        pipeline_id = last_pipeline["id"]

    discontinued_items = list(client.QueryItems(helix_collection_link,
                                       {
                                            'query': 'SELECT * FROM root r WHERE r.RuntimePipelineId=@pipeline_id',
                                            'parameters': [
                                                {'name': '@pipeline_id', 'value': pipeline_id}
                                            ]
                                       },
                                       {'enableCrossPartitionQuery': True}))

    print()


def bucket_results(client):
    print('\n1.3 - Reading all documents in a collection\n')
    helix_workitems_link = "dbs/coreclr-infra/colls/helix-workitems"
    helix_submission_link = "dbs/coreclr-infra/colls/helix-submissions"

    pipeline_link = "dbs/coreclr-infra/colls/runtime-pipelines"
    jobs_link = "dbs/coreclr-infra/colls/runtime-jobs"

    # Get only jobs from 8 may to 11 may
    c = list(client.QueryItems(jobs_link,
                                       {
                                            'query': 'SELECT * FROM root job WHERE job.DateStart>@min_start',
                                            'parameters': [
                                                {'name': '@min_start', 'value': "2020-05-08T02:30:52.635-07:00"}
                                            ]
                                       },
                                       {'enableCrossPartitionQuery': True}))

    pipeline_runs = list(client.ReadItems(pipeline_link, {'maxItemCount':1000}))
    #jobs = list(client.ReadItems(jobs_link, {'maxItemCount':1000}))
    helix_submissions = list(client.ReadItems(helix_submission_link, {'maxItemCount':1000}))
    helix_workitems = list(client.ReadItems(helix_workitems_link, {'maxItemCount':1000}))
    
    print('Found {0} helix submissions'.format(len(helix_submissions)))
    print('Found {0} helix workitems'.format(len(helix_workitems)))

    pipeline_id_to_run = defaultdict(lambda: None)
    for pipeline in pipeline_runs:
        assert pipeline["id"] not in pipeline_id_to_run
        pipeline_id_to_run[pipeline["id"]] = pipeline

    submission_by_pr_number = defaultdict(lambda: [])
    submissions_grouped_by_source = defaultdict(lambda: [])
    buckets = defaultdict(lambda: [])

    for submission in helix_submissions:
        submissions_grouped_by_source[submission["Source"]].append(submission)

        if "pull" in submission["Source"]:
            pr_number = submission["Source"].split("pull/")[1].split("/merge")[0]
            submission_by_pr_number[pr_number] = submission

    ci_work_items = submissions_grouped_by_source["ci/public/dotnet/runtime/refs/heads/master"]
    helix_queues_used = defaultdict(lambda: [])

    for item in ci_work_items:
        for queue in item["Queues"]:
            helix_queues_used[queue].append(item)

    jobs_categorized = defaultdict(defaultdict(lambda: []))
    for job in jobs:
        jobs_categorized[job["JobGuid"]][job["PipelineId"]].append(job)

    workitems_for_job = defaultdict(lambda: [])
    job_for_pipeline = defaultdict(lambda: [])

    job_id_to_job = defaultdict(lambda: None)
    for job in jobs:
        assert job["id"] not in job_id_to_job
        job_id_to_job[job["id"]] = job

    workitem_map = defaultdict(lambda: None)
    for workitem in helix_workitems:
        possible_jobs = jobs_categorized[workitem["JobId"]]

        found = False
        for job in possible_jobs:
            if job["PipelineId"] == workitem["RuntimePipelineId"]:
                found = True
                assert workitem["id"] not in workitem_map
                assert workitem["RuntimePipelineId"] in pipeline_id_to_run
                workitem_map[workitem["id"]] = (workitem, job, pipeline_id_to_run[workitem["RuntimePipelineId"]])

                workitems_for_job[job["id"]].append(workitem)

                break
            
        assert found

    for job_id in workitems_for_job:
        job = job_id_to_job[job_id]
        job_for_pipeline[job["PipelineId"]].append(job)

    sorted_pipelines = [item for item in job_for_pipeline]
    sorted_pipelines.sort()

    # Time spent in setup by pipeline
    for pipeline in sorted_pipelines:
        total_setup_time = 0
        total_run_time = 0

        workitem_count = 0

        helix_submission_jobs = job_for_pipeline[pipeline]

        # These are only jobs with helix submissions
        for job in helix_submission_jobs:
            workitems_in_job = workitems_for_job[job["id"]]

            workitem_count += len(workitems_in_job)

            for workitem in workitems_in_job:
                total_setup_time += workitem["ElapsedSetupTime"]
                total_run_time += workitem["ElapsedRunTime"]

        total_setup_time_seconds = total_setup_time / 1000
        total_run_time_seconds = total_run_time / 1000

        print("[{}] -- Workitems ({}). Total Setup Time ({}). Total Run Time ({})".format(pipeline, workitem_count, total_setup_time_seconds, total_run_time_seconds))

    i = 0

def get_last_runtime_pipeline(client):
    pipeline_link = "dbs/coreclr-infra/colls/runtime-pipelines"
    pipeline_runs = list(client.ReadItems(pipeline_link, {'maxItemCount':1000}))

    pipeline_runs.sort(key=lambda item: item["DateStart"])

    last_pipeline = pipeline_runs[-1]

    print("Last Ingested Pipeline: {}".format(last_pipeline["id"]))
    print("------------------------------------------------------")
    print()

    for key in last_pipeline:
        print("[{}]: {}".format(key, last_pipeline[key]))
    
def main():
    with CosmosDBGuard(cosmos_client.CosmosClient("https://coreclr-infra.documents.azure.com:443/", {'masterKey': os.environ["coreclrInfraKey"]} )) as client:
        try:
            #get_last_runtime_pipeline(client)
            bucket_results(client)

        except errors.HTTPFailure as e:
            print('Error. {0}'.format(e))

        print()

################################################################################
################################################################################

if __name__ == "__main__":
    main()