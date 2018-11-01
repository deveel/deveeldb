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
using System.Collections.Generic;
using System.Linq;
using System.Reflection;

using Deveel.Data.Services;

namespace Deveel.Data.Events {
	public static class ContextExtensions {
		internal static IEventSource GetEventSource(this IContext context) {
			var current = context;
			while (current != null) {
				if (current is IEventSource)
					return (IEventSource) current;

				current = current.ParentContext;
			}

			return null;
		}

		private static IEnumerable<IEventRegistry> FindRegistries(this IContext context, Type eventType) {
			return context.Scope.ResolveAll<IEventRegistry>()
				.Where(x => x.EventType.GetTypeInfo().IsAssignableFrom(eventType.GetTypeInfo()));
		}

		public static void RegisterEvent(this IContext context, Type eventType, params object[] args) {
			var source = context.GetEventSource();
			var registries = context.FindRegistries(eventType);
			foreach (var registry in registries) {
				registry.Register(eventType, source, args);
			}
		}

		private static IEnumerable<IEventRegistry<TEvent>> FindRegistries<TEvent>(this IContext context)
			where TEvent : class, IEvent {
			return context.Scope.ResolveAll<IEventRegistry<TEvent>>();
		}

		public static void RegisterEvent<TEvent>(this IContext context, params object[] args)
			where TEvent : class, IEvent {
			var source = context.GetEventSource();
			var registries = context.FindRegistries<TEvent>();
			foreach (var registry in registries) {
				registry.Register(source, args);
			}
		}
	}
}