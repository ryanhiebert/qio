Principles
==========

There's been a great deal of momentum to embrace async functions for IO,
but they split the world and are confusing, especially to new developers.
Threads or green threads are often a better choice for concurrency
because it doesn't require learning about coroutines
before you even need concurrency.

Still, async functions are a powerful way to implement state machines.
Complex workflows can be represented as async functions
to allow thinking about things in a pull-based fashion,
while also scaling well for processes that must take a long time.
Ideally, running coroutines could be serialized and resumed,
perhaps even on a different machine.

This vision motivates queueio.
Routines need to coordinate with the worker
because queue-level concurrency must be carefully managed.
Instead of complex construction of pipelines to make this possible,
queueio allows you to write complex workflows
in a traditional imperative style.

queueio encourages you to use synchronous IO in routines.
The yield points in async functions are only needed
when a routine needs to coordinate with the worker,
such as to indicate that the worker can use
the capacity reserved for it to do other work.
