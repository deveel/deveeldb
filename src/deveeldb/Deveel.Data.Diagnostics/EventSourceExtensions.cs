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
using System.Collections.Generic;

namespace Deveel.Data.Diagnostics {
	public static class EventSourceExtensions {
		public static void AppendMetadata(this IEventSource source, IEvent @event) {
			if (source == null)
				return;

			var sourceMetadata = new Dictionary<string, object>();
			var currentSource = source;
			while (currentSource != null) {
				var metadata = currentSource.Metadata;
				if (metadata != null) {
					foreach (var pair in metadata) {
						sourceMetadata[pair.Key] = pair.Value;
					}
				}

				currentSource = currentSource.ParentSource;
			}

			foreach (var pair in sourceMetadata) {
				@event.SetData(pair.Key, pair.Value);
			}
		}
	}
}
