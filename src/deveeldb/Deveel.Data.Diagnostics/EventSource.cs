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