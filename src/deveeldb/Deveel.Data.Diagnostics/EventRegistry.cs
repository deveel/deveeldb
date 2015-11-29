using System;
using System.Collections;
using System.Collections.Generic;
using System.Threading;
#if PCL
using System.Threading.Tasks;
#endif

using Deveel.Data.Configuration;

namespace Deveel.Data.Diagnostics {
	public sealed class EventRegistry : IEventRegistry, IDisposable {
		private readonly Queue<IEvent> eventsQueue;
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

		public EventRegistry(IContext context) {
			if (context == null)
				throw new ArgumentNullException("context");

			Context = context;

#if PCL
			cancellationTokenSource = new CancellationTokenSource();
			cancellationToken = cancellationTokenSource.Token;
#endif

			eventsQueue = new Queue<IEvent>();
			Start();
		}

		~EventRegistry() {
			Dispose(false);
		}

		public IContext Context { get; private set; }

		public void RegisterEvent(IEvent @event) {
			lock (((ICollection)eventsQueue).SyncRoot) {
				if (!route)
					return;

				eventsQueue.Enqueue(@event);
				reset.Set();
			}
		}

		private void RouteEvents() {
			while (route) {
				IEvent eventToRoute;

				lock (((ICollection) eventsQueue).SyncRoot) {
					reset.WaitOne();

					eventToRoute = eventsQueue.Dequeue();
				}

				var routers = Context.ResolveAllServices<IEventRouter>();
				foreach (var router in routers) {
					if (router.CanRoute(eventToRoute))
						router.RouteEvent(eventToRoute);
				}
			}
		}

		private void Start() {
			var config = Context.ResolveService<IConfiguration>();
			var threadCount = config.GetInt32("system.events.threads", 5);

#if PCL
			tasks = new List<Task>();
#else
			threads = new List<Thread>(threadCount);
#endif

			for (int i = 0; i < threadCount; i++) {
#if PCL
				tasks.Add(new Task(RouteEvents, cancellationToken));
#else
				var thread = new Thread(RouteEvents) {
					IsBackground = true,
					Name = String.Format("EventRegistry:{0} ({1})", Context.Name, i),
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

		public void Dispose() {
			Dispose(true);
			GC.SuppressFinalize(this);
		}

		private void Dispose(bool disposing) {
			if (disposed) {
				if (disposing) {
					Stop();
				}

#if PCL
				tasks = null;
#else
				threads = null;
#endif
				Context = null;
				disposed = true;
			}
		}
	}
}
