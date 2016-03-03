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

namespace Deveel.Data.Diagnostics {
	public static class ContextExtensions {
		public static void RegisterEvent(this IContext context, IEvent @event) {
			var currentContext = context;
			while (currentContext != null) {
				if (currentContext is IEventScope) {
					var scope = (IEventScope) currentContext;
					scope.EventRegistry.RegisterEvent(@event);
				}

				currentContext = currentContext.Parent;
			}
		}

		public static void AttachRouter(this IContext context, IEventRouter router) {
			var currentContext = context;
			while (currentContext != null) {
				if (currentContext is IEventScope) {
					currentContext.RegisterInstance(router);
					break;
				}

				currentContext = currentContext.Parent;
			}
		}

		public static void Route<TEvent>(this IContext context, Action<TEvent> router)
			where TEvent : class, IEvent {
			context.Route(router, null);
		}

		public static void Route<TEvent>(this IContext context, Action<TEvent> router, Func<TEvent, bool> condition)
			where TEvent : class, IEvent {
			context.AttachRouter(new DelegateRouter<TEvent>(router, condition));
		}

		#region DelegatedRouter

		class DelegateRouter<TEvent> : ThreadedQueue<TEvent>, IEventRouter where TEvent : class, IEvent {
			private Func<TEvent, bool> condition;
			private Action<TEvent> route;

			public DelegateRouter(Action<TEvent> route, Func<TEvent, bool> condition) {
				this.route = route;
				this.condition = condition;
			}

			protected override void Consume(TEvent message) {
				route(message);
			}

			public bool CanRoute(IEvent @event) {
				if (!(@event is TEvent))
					return false;

				if (condition == null)
					return true;

				return condition((TEvent)@event);
			}

			public void RouteEvent(IEvent e) {
				Enqueue((TEvent)e);
			}
		}

		#endregion
	}
}
