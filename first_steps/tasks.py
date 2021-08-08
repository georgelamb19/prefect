# Prefect refers to each step as a task
# in general, small tasks are better than monolithic ones
# tasks can optionally receive inputs and produce outputs

from prefect import task, Task

# the easiest way to create a new task is just by decorating an existing function

@task
def say_hello(person: str) -> None:
    print('Hello, {}!'.format(person))

# we can create task classes by subclassing the Prefect Task class
# ...and implementing its __init__() and run() methods

class AddTask(Task):

    def __init__(self, default: int, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.default = default

    def run(self, x: int, y: int=None) -> int:
        if y is None:
            y = self.default
        return x + y

# we need to initialise the task instance

add = AddTask(default=1)