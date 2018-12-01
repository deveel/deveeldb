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
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace Deveel.Data.Events {
	public sealed class InMemoryEventRegistry : IEventRegistry, IDisposable {
		private readonly List<IEventConsumer> consumers;
		private ConcurrentQueue<IEvent> queue;
		private Task[] threads;
		private CancellationTokenSource tokenSource;

		public InMemoryEventRegistry() {
			consumers = new List<IEventConsumer>();
			queue = new ConcurrentQueue<IEvent>();

			tokenSource = new CancellationTokenSource();

			threads = new Task[3];

			for (int i = 0; i < threads.Length; i++) {
				threads[i] = Task.Run(() => Listen(), tokenSource.Token);
			}
		}

		private void Listen() {
			while (!tokenSource.IsCancellationRequested) {
				if (queue.TryDequeue(out var @event)) {
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
			queue.Enqueue(@event);
		}

		public void AddConsumer(IEventConsumer consumer) {
			lock (consumers) {
				consumers.Add(consumer);
			}
		}

		public void Dispose() {
			while (!queue.IsEmpty) {
				Thread.Sleep(200);
			}

			if (tokenSource != null)
				tokenSource.Cancel();


			foreach (var thread in threads) {
				while (!thread.IsCompleted) {
					Thread.Sleep(100);
				}

				thread.Dispose();
			}
		}
	}
}