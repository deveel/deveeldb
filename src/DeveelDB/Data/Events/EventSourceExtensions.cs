// 
//  Copyright 2010-2017 Deveel
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

namespace Deveel.Data.Events {
	public static class EventSourceExtensions {
		public static T GetValue<T>(this IEventSource source, string key, CultureInfo formatProvider) {
			if (source == null || source.Metadata == null)
				return default(T);

			return source.Metadata.GetValue<T>(key, formatProvider);
		}

		public static T GetValue<T>(this IEventSource source, string key) {
			return source.GetValue<T>(key, CultureInfo.InvariantCulture);
		}
	}
}
