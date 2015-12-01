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
using System.Collections;
using System.Collections.Generic;

namespace Deveel.Data.Diagnostics {
	public sealed class LogEntry : IEnumerable<KeyValuePair<string, object>> {
		private readonly Dictionary<string, object> data;

		internal LogEntry(IDictionary<string, object> metadata) {
			data = new Dictionary<string, object>(metadata);
		}

		public IEnumerable<string> Keys {
			get { return data.Keys; }
		}

		public object GetValue(string key) {
			object value;
			if (!data.TryGetValue(key, out value))
				return null;

			return value;
		}

		public IEnumerator<KeyValuePair<string, object>> GetEnumerator() {
			return data.GetEnumerator();
		}

		IEnumerator IEnumerable.GetEnumerator() {
			return GetEnumerator();
		}

		public static LogEntry FromEvent(IEvent @event) {
			var data = new Dictionary<string, object>();
			if (@event.EventSource != null) {
				var source = @event.EventSource;
				while (source != null) {
					var sourceData = source.Metadata;
					foreach (var pair in sourceData) {
						data[pair.Key] = pair.Value;
					}

					source = source.ParentSource;
				}
			}

			foreach (var pair in @event.EventData) {
				data[pair.Key] = pair.Value;
			}

			return new LogEntry(data);
		}
	}
}
