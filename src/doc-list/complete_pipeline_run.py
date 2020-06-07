################################################################################
#
# Module: complete_pipeline_run.py
#
# Notes:
#
# Finding all of the jobs and workitems for a pipeline is an expensive process.
# It will touch ~50 thousand documents in cosmosdb.
#
################################################################################

from collections import defaultdict

################################################################################
# Helper methods
################################################################################


################################################################################
# Class
################################################################################

class complete_pipeline_run:
    
    def __init__(self, cosmodb_client, pipeline):
        """ Constructor
        """
        self.helix_workitems_link = "dbs/coreclr-infra/colls/helix-workitems"
        self.helix_submission_link = "dbs/coreclr-infra/colls/helix-submissions"

        self.pipeline_link = "dbs/coreclr-infra/colls/runtime-pipelines"
        self.jobs_link = "dbs/coreclr-infra/colls/runtime-jobs"

        self.failed_jobs = []
        self.jobs = []

        self.failed_work_items = []
        self.work_items = []

        # Seconds
        self.total_setup_time = 0
        self.total_run_time = 0

        self.total_work_items = 0
        self.total_tests_run = 0
        self.total_test_failures = 0

        self.pipeline = pipeline

    ############################################################################
    # Member methods
    ############################################################################

    ############################################################################
    # Helper methods
    ############################################################################
