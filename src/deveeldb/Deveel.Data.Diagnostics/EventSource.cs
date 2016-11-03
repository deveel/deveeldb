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
using System.Collections.Generic;
using System.Linq;

namespace Deveel.Data.Diagnostics {
	public class EventSource : IEventSource {
		private Dictionary<string, object> sourceMetadata;

		public EventSource() 
			: this(null) {
		}

		public EventSource(IEventSource parentSource) {
			ParentSource = parentSource;
		}

		IEventSource IEventSource.ParentSource {
			get { return ParentSource; }
		}

		protected IEventSource ParentSource { get; private set; }

		protected virtual bool CacheMetadata {
			get { return false; }
		}

		IEnumerable<KeyValuePair<string, object>> IEventSource.Metadata {
			get {
				if (sourceMetadata == null) {
					var meta = new Dictionary<string, object>();

					if (ParentSource != null) {
						var parentMeta = ParentSource.Metadata;
						foreach (var pair in parentMeta) {
							meta[pair.Key] = pair.Value;
						}
					}

					GetMetadata(meta);

					if (!CacheMetadata)
						return meta.AsEnumerable();

					sourceMetadata = meta;
				}

				return sourceMetadata.AsEnumerable();
			}
		}

		protected virtual void GetMetadata(Dictionary<string, object> metadata) {
		}

		internal void CopyFrom(IEventSource eventSource) {
			if (eventSource != null && eventSource.Metadata != null) {
				sourceMetadata = new Dictionary<string, object>(eventSource.Metadata.ToDictionary(x => x.Key, y => y.Value));
			}
		}
	}
}