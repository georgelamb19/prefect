# Prefect Core ships with an open-source server and UI for orchestrating and managing flows
# the local server stores flow metadata in a Postgres database and exposes a GraphQL API
# the local server requires Docker and Docker Compose to be installed
# this script builds a flow, then registers the flow with the local server

###########################################
# NOTES TO READ BEFORE RUNNING THIS SCRIPT
# before running this script we need to start and configure Core's server
# open a new terminal and run the following from the CLI, with our pipenv virtual environment activated:
#   prefect backend server
#   prefect server start
# after starting and configuring the server, we can navigate to http://localhost:8080 to see the Prefect UI
# we can then run this script to build a flow, and register it with the local backend
# after registering the flow we can:
# - use the URL returned from the register() call to navigate directly to the flow in the UI
# - start a local agent by running 'prefect agent local start' in the CLI, with our pipenv virtual environment activated
# - (the local agent communicates between the server and this flow code)
# - trigger a flow run from the UI by pressing the 'Run' button
###########################################

from tasks import say_hello, add
from prefect import Flow, Parameter, Client

# first we build a flow

second_add = add.copy()
with Flow('flow for orchestration') as flow:
    name = Parameter('name')
    first_result = add(1, y=2)
    second_result = second_add(x=first_result, y=100)
    say_hello(person=name, upstream_tasks=[second_add])

# now we register the flow

client = Client()
client.create_project(project_name="first_project")
flow.register(project_name="first_project")