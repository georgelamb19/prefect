# flows are used to describe the dependencies between tasks
# if tasks are like functions, we can think of a flow as a script that combines them

# when you build a flow in Prefect, you're defining a computational graph that can be executed in the future
# the pattern is always the same: step 1 is to build a flow, step 2 is to run() the flow

from tasks import say_hello, add
from prefect import Flow, Parameter


## Flow 1, to demonstrate Prefect's functional API

# the easiest way to build a flow is with Prefect's functional API
# we create a flow as a context manager and call tasks on each other
# Prefect builds up a computational graph that represents the workflow
# note: no tasks are actually executed at this time

with Flow('My first flow!') as flow:
    first_result = add(1, y=2)
    second_result = add(x=first_result, y=100)

# once the flow has been created, we can execute it by calling flow.run()
# we can examine the state of the run, and the state and result of each task

state = flow.run()

assert state.is_successful()

first_task_state = state.result[first_result]
assert first_task_state.is_successful()
assert first_task_state.result == 3

second_task_state = state.result[second_result]
assert second_task_state.is_successful()
assert second_task_state.result == 103

# the run() method handles scheduling, retries, data serialisation etc
# note: in production execution of flow.run() would probably be handled by a management API


## Flow 2, to demonstrate the concept of parameters

# Prefect provides a special task called a Parameter
# Parameters enable us to provide information to a flow at runtime

with Flow('Say hi!') as flow:
    n = Parameter('name')
    say_hello(n)

flow.run(name='Ed')


## Flow 3, to demonstrate Prefect's imperative API

# the functional API makes it easy to define workflows in a script-like style
# the imperative API enables us to build flows in a more programmatic or explicit way

flow = Flow('My imperative flow!')

# define some new tasks
name = Parameter('name')
second_add = add.copy()

# add our tasks to the flow
flow.add_task(add)
flow.add_task(second_add)
flow.add_task(say_hello)

# create non-data dependencies so that `say_hello` waits for `second_add` to finish
say_hello.set_upstream(second_add, flow=flow)

# create data bindings
add.bind(x=1, y=2, flow=flow)
second_add.bind(x=add, y=100, flow=flow)
say_hello.bind(person=name, flow=flow)

flow.run(name='Chris')

# note: with the functional API, the upstream_tasks keyword arg can be used to define state-dependencies
# note: you can switch between the functional API and the imperative API at any time