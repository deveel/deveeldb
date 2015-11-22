using System;
using System.Collections;
using System.Collections.Generic;
using System.Threading;

using Deveel.Data.Configuration;
using Deveel.Data.Services;

namespace Deveel.Data.Diagnostics {
	public sealed class EventRegistry : IEventRegistry, IDisposable {
		private readonly Queue<IEvent> eventsQueue;
		private List<Thread> threads;
		private bool route;

		private AutoResetEvent reset;

		private bool disposed;

		public EventRegistry(IContext context) {
			if (context == null)
				throw new ArgumentNullException("context");

			Context = context;

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

			threads = new List<Thread>(threadCount);

			for (int i = 0; i < threadCount; i++) {
				var thread = new Thread(RouteEvents) {
					IsBackground = true,
					Name = String.Format("EventRegistry:{0} ({1})", Context.Name, i),
					Priority = ThreadPriority.AboveNormal
				};

				threads.Add(thread);
			}

			route = true;
			reset = new AutoResetEvent(false);

			foreach (var thread in threads) {
				thread.Start();
			}
		}

		private void Stop() {
			route = false;

			foreach (var thread in threads) {
				try {
					thread.Join(300);
					thread.Interrupt();
				} catch (ThreadInterruptedException) {
				}
			}
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

				threads = null;
				Context = null;
				disposed = true;
			}
		}
	}
}
