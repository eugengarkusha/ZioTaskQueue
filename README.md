# ZioTaskQueue
Implementation of the cancellable tasks queue using ZIO

#### Supported oprations:

- __add__ task by key. Returns promise of completion result 
- __cancel__ task by key, supports label to identify the origin of the call. Returns promise of cancellation result  
- __join__ task by key. Returns promise of join result. 
- __getRegisteredTaskKeys__. Returns all keys of the tasks which are either in progress or registered for execution

#### Completion result types: 

- Done(result)  
- Cancelled(origin)  
- Failed(errors)  
- Died(defects)  
- Duplicate  

#### Cancellation/Join result types: 

- Done(result)  
- Cancelled(origin)  
- Failed(errors)  
- Died(defects)  
- NotFound  
