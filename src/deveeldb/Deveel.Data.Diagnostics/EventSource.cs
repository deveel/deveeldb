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
		public EventSource(IContext context) 
			: this(context, null) {
		}

		public EventSource(IContext context, IEventSource parentSource) {
			if (context == null)
				throw new ArgumentNullException("context");

			Context = context;
			ParentSource = parentSource;
		}

		public IContext Context { get; private set; }

		public IEventSource ParentSource { get; private set; }

		public IEnumerable<KeyValuePair<string, object>> Metadata {
			get {
				var metadata = new Dictionary<string, object>();
				GetMetadata(metadata);
				return metadata.AsEnumerable();
			}
		}

		protected virtual void GetMetadata(Dictionary<string, object> metadata) {
		}
	}
}