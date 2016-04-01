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
			where TEvent : IEvent {
			context.Route(router, null);
		}

		public static void Route<TEvent>(this IContext context, Action<TEvent> router, Func<TEvent, bool> condition)
			where TEvent : IEvent {
			context.AttachRouter(new DelegateRouter<TEvent>(router, condition));
		}

		public static void RouteImmediate<TEvent>(this IContext context, Action<TEvent> router, Func<TEvent, bool> condition)
			where TEvent : IEvent {
			context.AttachRouter(new ImmediateRouter<TEvent>(router, condition));
		}

		public static void RouteImmediate<TEvent>(this IContext context, Action<TEvent> router)
			where TEvent : IEvent {
			context.RouteImmediate(router, e => true);
		}

		#region DelegatedRouter

		class DelegateRouter<TEvent> : ThreadedQueue<TEvent>, IEventRouter where TEvent : IEvent {
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

		#region ImmediateRouter

		class ImmediateRouter<TEvent> : IEventRouter {
			private Func<TEvent, bool> condition;
			private Action<TEvent> router;

			public ImmediateRouter(Action<TEvent> router, Func<TEvent, bool> condition) {
				this.router = router;
				this.condition = condition;
			}

			public bool CanRoute(IEvent @event) {
				if (!(@event is TEvent))
					return false;

				return condition((TEvent) @event);
			}

			public void RouteEvent(IEvent e) {
				router((TEvent) e);
			}
		}

		#endregion
	}
}
