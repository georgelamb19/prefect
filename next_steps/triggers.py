# TRIGGERS
# normal data pipeline rules: when a task succeeds, its downstream dependencies start running
# more complex requirement: a 'clean up task' that must run even if a prior task has failed
# solution is a concept called triggers:
# - before a task runs, its trigger function is called on a set of upstream task states
# - task runs if trigger function passes
# - Prefect has built in triggers: all_successful (default), all_failed, any_successful, any_field, manual_only

# REFERENCE TASKS
# Flows remain in a 'Running' state until all of their tasks enter 'Finished' states
# A Flow decided its own final state by applying an all_successful trigger to its reference tasks
# By default, the reference tasks are the terminal tasks, but users can customise

from prefect import task, Flow, Parameter, triggers

# define tasks

@task
def create_cluster(name):
    return name+'\'s spark cluster'

@task
def run_spark_job(cluster):
    #print(cluster+' with job')
    print(cluster+4) # simulating a failed job

@task(trigger=triggers.always_run) # use the always_run trigger, an alias for all_finished
def tear_down_cluster(cluster):
    print(cluster+' torn down')

# define flow

with Flow("Spark") as flow:

    # define data dependencies
    name = Parameter('name')
    cluster = create_cluster(name)
    submitted = run_spark_job(cluster)
    result = tear_down_cluster(cluster)

    # wait for the job to finish before tearing down the cluster
    result.set_upstream(submitted)

# use run_spark_job task as reference task
flow.set_reference_tasks([submitted])

flow.run(name='Simon')

# note: a trigger function evaluates both data and non-data dependencies
# for example, if the create_cluster task above were also to fail:
# - run_spark_job would have final state 'TriggerFailed'
# - tear_down_cluster could have final state 'Failed'