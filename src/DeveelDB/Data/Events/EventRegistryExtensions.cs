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
using System.Reflection;

using Deveel.Util;

namespace Deveel.Data.Events {
	public static class EventRegistryExtensions {
		private static IEvent CreateEvent(Type eventType, IEventSource source, params object[] args) {
			var ctorArgs = ArrayUtil.Introduce(source, args);
			return Activator.CreateInstance(eventType, ctorArgs) as IEvent;
		}

		public static void Register(this IEventRegistry registry, Type type, IEventSource source, params object[] args) {
			if (type == null)
				throw new ArgumentNullException(nameof(type));
			if (source == null)
				throw new ArgumentNullException(nameof(source));

			if (!typeof(IEvent).GetTypeInfo().IsAssignableFrom(type.GetTypeInfo()))
				throw new ArgumentException($"The type '{type}' is not assignable from '{typeof(IEvent)}'.");

			var @event = CreateEvent(type, source, args);

			if (registry != null)
				registry.Register(@event);
		}

		public static void Register<TEvent>(this IEventRegistry registry, IEventSource source, params object[] args)
			where TEvent : class, IEvent {
			registry.Register(typeof(TEvent), source, args);
		}
	}
}