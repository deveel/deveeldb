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
			Start();
		}

		~ThreadedQueue() {
			Dispose(false);
		}

		public virtual int ThreadCount {
			get { return DefaultThreadCount; }
		}

		private void Start() {

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
					Priority = ThreadPriority.AboveNormal
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
		}

		private void Stop() {
			route = false;

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
					thread.Join(300);
					thread.Interrupt();
				} catch (ThreadInterruptedException) {
				}
			}
#endif
		}

		private void RouteMessages() {
			while (route) {
				TMessage message;

				lock (((ICollection) messageQueue).SyncRoot) {
					reset.WaitOne();

					message = messageQueue.Dequeue();
				}

				Consume(message);
			}
		}

		protected abstract void Consume(TMessage message);

		protected virtual void Enqueue(TMessage message) {
			lock (((ICollection) messageQueue).SyncRoot) {
				if (!route)
					return;

				messageQueue.Enqueue(message);
				reset.Set();
			}
		}

		public void Dispose() {
			Dispose(true);
			GC.SuppressFinalize(this);
		}

		protected virtual void Dispose(bool disposing) {
			if (disposed) {
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