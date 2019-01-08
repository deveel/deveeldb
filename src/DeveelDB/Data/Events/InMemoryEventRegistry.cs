// 
//  Copyright 2010-2018 Deveel
// 
//    Licensed under the Apache License, Version 2.0 (the "License");
//    you may not use this file except in compliance with the License.
//    You may obtain a copy of the License at
// 
//        http://www.apache.org/licenses/LICENSE-2.0
// 
//    Unless required by applicable law or agreed to in writing, software
//    distributed under the License is distributed on an "AS IS" BASIS,
//    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//    See the License for the specific language governing permissions and
//    limitations under the License.
//

using System;
using System.Collections;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace Deveel.Data.Events {
	public class InMemoryEventRegistry : IEventRegistry, IDisposable {
		private readonly List<IEventConsumer> consumers;
		private Queue<IEvent> queue;
		private readonly AutoResetEvent semaphore;
		private Task[] threads;
		private CancellationTokenSource tokenSource;

		public InMemoryEventRegistry() {
			consumers = new List<IEventConsumer>();
			queue = new Queue<IEvent>();

			tokenSource = new CancellationTokenSource();

			threads = new Task[3];

			for (int i = 0; i < threads.Length; i++) {
				threads[i] = Task.Run(() => Listen(), tokenSource.Token);
			}

			semaphore = new AutoResetEvent(false);
		}

		private void Listen() {
			while (!tokenSource.IsCancellationRequested) {
				if (semaphore.WaitOne(100)) {
					IEvent @event;

					lock (((ICollection)queue).SyncRoot) {
						@event = queue.Dequeue();
					}

					IEnumerable<IEventConsumer> currentConsumers;

					lock (consumers) {
						currentConsumers = new List<IEventConsumer>(consumers);
					}

					foreach (var consumer in currentConsumers) {
						consumer.Consume(@event);
					}
				}
			}
		}

		public void Register(IEvent @event) {
			lock (((ICollection)queue).SyncRoot) {
				queue.Enqueue(@event);
			}

			semaphore.Set();

			OnEventRegistered(@event);
		}

		protected virtual void OnEventRegistered(IEvent @event) {

		}
 
		public void AddConsumer(IEventConsumer consumer) {
			lock (consumers) {
				consumers.Add(consumer);
			}
		}

		public void Dispose() {
			if (tokenSource != null)
				tokenSource.Cancel();

			semaphore?.Dispose();

			foreach (var thread in threads) {
				while (!thread.IsCompleted) {
					Thread.Sleep(100);
				}

				thread.Dispose();
			}
		}
	}
}