// 
//  Copyright 2010-2016 Deveel
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
using System.Collections.Generic;
using System.Threading;
#if PCL
using System.Threading.Tasks;
#endif

namespace Deveel.Data.Diagnostics {
	public abstract class ThreadedQueue<TMessage> : IDisposable where TMessage : class {
		private readonly Queue<TMessage> messageQueue;
#if PCL
		private CancellationTokenSource cancellationTokenSource;
		private CancellationToken cancellationToken;
		private List<Task> tasks; 
#else
		private List<Thread> threads;
#endif
		private bool started;
		private bool route;
		private AutoResetEvent reset;

		private bool disposed;

		public const int DefaultThreadCount = 2;

		protected ThreadedQueue() {
#if PCL
			cancellationTokenSource = new CancellationTokenSource();
			cancellationToken = cancellationTokenSource.Token;
#endif

			messageQueue = new Queue<TMessage>();
		}

		~ThreadedQueue() {
			Dispose(false);
		}

		public virtual int ThreadCount {
			get { return DefaultThreadCount; }
		}

		private void Start() {
			if (started)
				return;

#if PCL
			tasks = new List<Task>();
#else
				threads = new List<Thread>(ThreadCount);
#endif

			for (int i = 0; i < ThreadCount; i++) {
#if PCL
				tasks.Add(new Task(RouteMessages, cancellationToken));
#else
					var thread = new Thread(RouteMessages) {
						IsBackground = true,
						Name = String.Format("{0}.QueueConsumer {1}", GetType().Name, i)
					};

					threads.Add(thread);
#endif
			}

			route = true;
			reset = new AutoResetEvent(false);

#if PCL
			foreach (var task in tasks) {
				task.Start();
			}
#else
				foreach (var thread in threads) {
					thread.Start();
				}
#endif
			started = true;
		}

		private void Stop() {
			route = false;

			if (!started)
				return;

#if PCL
			cancellationTokenSource.Cancel(true);

			foreach (var task in tasks) {
				try {
					task.Wait(300);
				} catch (TaskCanceledException) {
				}
			}
#else
			foreach (var thread in threads) {
				try {
					thread.Join(50);
					thread.Interrupt();
				} catch (ThreadInterruptedException) {
				}
			}
#endif

			started = false;
		}

		private void RouteMessages() {
			while (route) {
				reset.WaitOne();

				TMessage message;

				lock (((ICollection) messageQueue).SyncRoot) {
					message = messageQueue.Dequeue();
				}

				Consume(message);
			}
		}

		protected abstract void Consume(TMessage message);

		protected void Enqueue(TMessage message) {
			Start();

			if (!route)
				return;

			lock (((ICollection) messageQueue).SyncRoot) {
				messageQueue.Enqueue(message);
			}

			reset.Set();
		}

		public void Dispose() {
			Dispose(true);
			GC.SuppressFinalize(this);
		}

		protected virtual void Dispose(bool disposing) {
			if (!disposed) {
				if (disposing) {
					Stop();
				}

#if PCL
				tasks = null;
#else
				threads = null;
#endif
				disposed = true;
			}
		}
	}
}