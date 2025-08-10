import time
import random
import threading
from queue import Queue
from concurrent.futures import ThreadPoolExecutor, as_completed

# Mock GraphQL Adapter (simulate network calls)
class GraphQLAdapter:
    def run_mutation(self, id_: int) -> str:
        # Simulate network delay
        time.sleep(random.uniform(0.2, 0.5))
        return f"task-{id_}"

    def check_status(self, task_id: str) -> str:
        # Simulate variable status check times
        time.sleep(random.uniform(0.5, 1.5))
        return f"completed-{task_id}"

# Manager to coordinate mutation and status checking
class GraphQLBatchProcessor:
    def __init__(self, adapter: GraphQLAdapter, mutation_workers=5, status_workers=5):
        self.adapter = adapter
        self.task_queue = Queue()
        self.mutation_pool = ThreadPoolExecutor(max_workers=mutation_workers)
        self.status_pool = ThreadPoolExecutor(max_workers=status_workers)
        self.running = True

    def run_batch_mutations(self, batch):
        futures = []
        for id_ in batch:
            future = self.mutation_pool.submit(self._mutation_task, id_)
            futures.append(future)

        # As each mutation finishes, its task_id is put in the queue for status check
        for future in as_completed(futures):
            task_id = future.result()
            self.task_queue.put(task_id)

    def _mutation_task(self, id_):
        task_id = self.adapter.run_mutation(id_)
        print(f"Mutation done for id={id_}, task_id={task_id}")
        return task_id

    def _status_worker(self):
        while self.running or not self.task_queue.empty():
            try:
                task_id = self.task_queue.get(timeout=1)
            except:
                continue
            self.status_pool.submit(self._check_status_task, task_id)

    def _check_status_task(self, task_id):
        result = self.adapter.check_status(task_id)
        print(f"Status check complete for {task_id}: {result}")

    def start_status_listener(self):
        threading.Thread(target=self._status_worker, daemon=True).start()

    def shutdown(self):
        self.running = False
        self.mutation_pool.shutdown(wait=True)
        self.status_pool.shutdown(wait=True)


# Simulated batches of IDs
batches = [
    [1, 2, 3, 4, 5],
    [6, 7, 8, 9, 10],
    [11, 12, 13, 14, 15]
]

# Run the processor
adapter = GraphQLAdapter()
processor = GraphQLBatchProcessor(adapter)
processor.start_status_listener()

for batch in batches:
    processor.run_batch_mutations(batch)
    time.sleep(1)  # simulate time gap before next batch

# Allow remaining status checks to finish
processor.shutdown()
