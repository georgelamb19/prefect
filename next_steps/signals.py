# STATES
# states are the 'currency' of Prefect
# all information about tasks and flows is transmitted via rich State objects
# at any moment, you can learn anything you need to know about a task/flow through its current state / state history
# note: ONLY RUNS HAVE STATES
# - flows and tasks are templates that describe what a system does
# - only when we run the system does it also take on a state
# - if we refer to a task as 'running', we really mean that a specific instance of the task is in that state

# SIGNALS
# Prefect's State system allows users to set up advanced behaviours through triggers and reference tasks
# normally, if function finishes normally - 'Success' state; else if it encounters an error - 'Failed' state
# signals are ways of telling the Prefect engine that a task should be moved immediately into a specific state
# signals:
# - raise signals.SUCCESS
# - raise signals.FAIL
# - raise signals.RETRY (causes task to immediately retry itself)
# - raise signals.PAUSE (task enters Paused state until explicitly resumed)
# - raise signals.SKIP
#   - when a task is skipped, downstream tasks also skipped unless skip_on_upstream_skip=False
#   - when a task is skipped, it's usually treated as if it ran successfully

from prefect import task, Flow, Parameter
from prefect.engine import signals

# we raise signals inside a task's run() function

@task
def signal_task(message):
    if message == 'go!':
        raise signals.SUCCESS(message='going!')
    elif message == 'stop!':
        raise signals.FAIL(message='stopping!')
    elif message == 'skip!':
        raise signals.SKIP(message='skipping!')

with Flow('signal flow') as flow:
    message = Parameter('message')
    signal_task(message)

flow.run(message='skip!')