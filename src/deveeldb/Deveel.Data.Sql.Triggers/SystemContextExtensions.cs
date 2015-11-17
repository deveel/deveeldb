// 
//  Copyright 2010-2015 Deveel
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

using Deveel.Data.Diagnostics;
using Deveel.Data.Services;

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
