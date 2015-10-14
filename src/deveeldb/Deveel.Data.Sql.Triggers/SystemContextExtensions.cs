using System;

using Deveel.Data.Diagnostics;

namespace Deveel.Data.Sql.Triggers {
	public static class SystemContextExtensions {
		public static void UseTriggerListener(this ISystemContext context, ITriggerListener listener) {
			var router = context.ResolveService<TriggerEventRouter>();
			if (router == null)
				context.RegisterService<TriggerEventRouter>();

			context.ServiceProvider.Register(listener);
		}

		public static void ListenTriggers(this ISystemContext context, Action<TriggerEvent> listener) {
			context.UseTriggerListener(new DelegatedTriggerListener(context, listener));
		}

		#region DelegatedTriggerListener

		private class DelegatedTriggerListener : ITriggerListener, IDisposable {
			private Action<TriggerEvent> listener;
			private ISystemContext systemContext;

			public DelegatedTriggerListener(ISystemContext systemContext, Action<TriggerEvent> listener) {
				this.systemContext = systemContext;
				this.listener = listener;
			}

			public void OnTriggerEvent(TriggerEvent trigger) {
				try {
					if (listener != null)
						listener(trigger);
				} catch (Exception ex) {
					// TODO: form a source...
					systemContext.EventRegistry.Error(null, ex);
				}
			}

			public void Dispose() {
				listener = null;
				systemContext = null;
			}
		}

		#endregion
	}
}
