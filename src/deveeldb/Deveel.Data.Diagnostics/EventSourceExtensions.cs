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
