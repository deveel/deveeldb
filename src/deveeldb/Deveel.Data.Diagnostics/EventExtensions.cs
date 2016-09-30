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
using System.Globalization;

namespace Deveel.Data.Diagnostics {
	public static class EventExtensions {
		public static EventMessage AsMessage(this IEvent @event) {
			return new EventMessage(@event);
		}

		public static T GetData<T>(this IEvent @event, string key, IFormatProvider formatProvider) {
			if (@event == null || @event.EventData == null)
				return default(T);

			return @event.EventData.GetValue<T>(key, formatProvider);
		}

		public static T GetData<T>(this IEvent @event, string key) {
			return @event.GetData<T>(key, CultureInfo.InvariantCulture);
		}

		public static string DatabaseName(this IEvent @event) {
			return @event.EventSource.DatabaseName();
		}

		public static string OsPlatform(this IEvent @event) {
			return @event.EventSource.OsPlatform();
		}
	}
}
