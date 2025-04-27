# Worker Lifecycle Events

This document lists the key events used to coordinate the lifecycle of the worker and its components.

## Core Worker Events
- `WorkerStarted` - When the worker begins running
- `WorkerStopped` - When the worker stops running
- `WorkerShutdownInitiated` - When shutdown is requested
- `WorkerShutdownCompleted` - When shutdown is fully complete

## Component Events
- `ConsumerStarted` - When the consumer thread begins consuming
- `ConsumerStopped` - When the consumer thread stops
- `ContinuerStarted` - When the continuer thread begins running
- `ContinuerStopped` - When the continuer thread stops
- `StarterStarted` - When the starter thread begins running
- `StarterStopped` - When the starter thread stops

## Resource Management Events
- `ConcurrencyReserved` - When concurrency is successfully reserved
- `ConcurrencyReleased` - When concurrency is released
- `TaskQueueEmpty` - When the task queue becomes empty
- `TaskQueueFull` - When the task queue reaches capacity

## Connection Events
- `ConsumerConnectionLost` - When the consumer loses connection
- `ConsumerConnectionRestored` - When the consumer reconnects

These events help coordinate:
- Graceful shutdown coordination
- Health monitoring
- Debugging and logging
- State management between threads
- Error recovery scenarios
