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
using System.Linq;

namespace Deveel.Data.Diagnostics {
	public static class EventSourceExtensions {
		public static T GetMetadata<T>(this IEventSource source, string key, CultureInfo formatProvider) {
			if (String.IsNullOrEmpty(key))
				throw new ArgumentNullException("key");

			if (source == null || source.Metadata == null)
				return default(T);

			var meta = source.Metadata.ToDictionary(pair => pair.Key, pair => pair.Value);
			object value;
			if (!meta.TryGetValue(key, out value) ||
				value == null)
				return default(T);

			if (!(value is T)) {
				if (formatProvider == null)
					throw new ArgumentNullException("formatProvider");

				var nullableType = Nullable.GetUnderlyingType(typeof(T));
				if (nullableType != null) {
					value = Convert.ChangeType(value, nullableType, formatProvider);
				} else {
					value = Convert.ChangeType(value, typeof(T), formatProvider);
				}
			}

			return (T) value;
		}

		public static T GetMetadata<T>(this IEventSource source, string key) {
			return source.GetMetadata<T>(key, CultureInfo.InvariantCulture);
		}

		public static string OsPlatform(this IEventSource source) {
			return source.GetMetadata<string>(MetadataKeys.System.Environment.OsPlatform);
		}

		public static string OsVersion(this IEventSource source) {
			return source.GetMetadata<string>(MetadataKeys.System.Environment.OsVersion);
		}

		public static string SessionLastCommandText(this IEventSource source) {
			return source.GetMetadata<string>(MetadataKeys.Session.LastCommandText);
		}

		public static DateTimeOffset? SessionLastCommandTime(this IEventSource source) {
			return source.GetMetadata<DateTimeOffset?>(MetadataKeys.Session.LastCommandTime);
		}

		public static DateTimeOffset? SessionStartTimeUtc(this IEventSource source) {
			return source.GetMetadata<DateTimeOffset?>(MetadataKeys.Session.StartTimeUtc);
		}

		public static string SessionTimeZone(this IEventSource source) {
			return source.GetMetadata<string>(MetadataKeys.Session.TimeZone);
		}

		public static string DatabaseName(this IEventSource source) {
			return source.GetMetadata<string>(MetadataKeys.Database.Name);
		}

		public static int DatabaseSessionCount(this IEventSource source) {
			return source.GetMetadata<int>(MetadataKeys.Database.SessionCount);
		}
	}
}
