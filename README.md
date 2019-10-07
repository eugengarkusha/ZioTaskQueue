# ZioTaskQueue
Implementation of the cancellable tasks queue using ZIO

#### Supported oprations:

- __add__ task by key. Returns the task result. Locks fiber until result is ready.
- __cancel__ task by key, supports label to identify the origin of the call. Returns the task cancellation result. Locks fiber until result is cancelled.  
- __join__ task by key. Returns join result. Locks fiber until result is ready.
- __getRegisteredTaskKeys__. Returns all keys of the tasks which are either in progress or registered for execution
- __add___  and __cancel___ : versions of the respective operations which ignore the results. Return without waiting for completion.


#### Completion result types: 

- Done(result)  
- Cancelled(origin)  
- Failed(errors)  
- Died(defects)  
- Duplicate  
- Rejected  

#### Cancellation/Join result types: 

- Done(result)  
- Cancelled(origin)  
- Failed(errors)  
- Died(defects)  
- NotFound  
