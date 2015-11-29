using System;

using Deveel.Data.Configuration;

namespace Deveel.Data.Diagnostics {
	public sealed class EventRegistry : ThreadedQueue<IEvent>, IEventRegistry, IDisposable {
		private bool disposed;

		private readonly int threadCount;

		public EventRegistry(IContext context) {
			if (context == null)
				throw new ArgumentNullException("context");

			Context = context;

			var config = context.ResolveService<IConfiguration>();
			threadCount = config.GetInt32("system.events.threadCount", 4);
		}

		~EventRegistry() {
			Dispose(false);
		}

		public IContext Context { get; private set; }

		public override int ThreadCount {
			get { return threadCount; }
		}

		public void RegisterEvent(IEvent @event) {
			Enqueue(@event);
		}

		protected override void Consume(IEvent message) {
			var routers = Context.ResolveAllServices<IEventRouter>();
			foreach (var router in routers) {
				try {
					if (router.CanRoute(message))
						router.RouteEvent(message);
				} catch (Exception ex) {
					Enqueue(new ErrorEvent(ex, -1, ErrorLevel.Critical));
				}
			}
		}

		protected override void Dispose(bool disposing) {
			if (disposed) {
				Context = null;
				disposed = true;
			}

			base.Dispose(disposing);
		}
	}
}
