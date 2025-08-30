qio
===

![qio Logo](logo.png)

Python background queue processing with an async twist.

Principles
----------

There's been a great deal of momentum to embrace async functions,
but they are confusing and split the world.
Threads or green threads are often a better choice
for common workloads.

However, coroutines are a powerful way to implement state machines.
Complex workflows can be represented as coroutines
to allow thinking about things in a pull-based fashion,
while also scaling well for processes that must take a long time.
Ideally, coroutines could be serialized and resumed,
perhaps even on a different machine.

It is with this in mind that qio was created.
It replaces other tools for complex workflows
such as Celery signatures,
and instead allows you to write complex workflows
in a traditional imperative style.

qio encourages you to use synchronous IO in routines.
Your routines are run on an isolated thread,
but when they need to pause to call other routines,
they can `await` those routines so that quo can
continue processing other tasks,
including the ones you're waiting on,
so that the queue processing won't be blocked.
