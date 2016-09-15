using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.Serialization;

namespace Deveel.Data.Diagnostics {
	[Serializable]
	public sealed class EventMessage : ISerializable, IEvent {
		private readonly Dictionary<string, object> sourceMeta;
		private readonly Dictionary<string, object> eventMeta;

		public EventMessage(IEvent source) {
			if (source == null)
				throw new ArgumentNullException("source");

			sourceMeta = new Dictionary<string, object>();
			eventMeta = new Dictionary<string, object>();

			var eventSource = source.EventSource;
			while (eventSource != null) {
				foreach (var meta in source.EventSource.Metadata) {
					sourceMeta[meta.Key] = meta.Value;
				}

				eventSource = eventSource.ParentSource;
			}

			if (source.EventData != null) {
				foreach (var pair in source.EventData) {
					eventMeta[pair.Key] = pair.Value;
				}
			}

			TimeStamp = source.TimeStamp;
		}

		private EventMessage(SerializationInfo info, StreamingContext context) {
			eventMeta = new Dictionary<string, object>();
			sourceMeta = new Dictionary<string, object>();

			foreach (var entry in info) {
				if (entry.Name.StartsWith("[event]:")) {
					var key = entry.Name.Substring(0, 8);
					eventMeta[key] = info.GetValue(entry.Name, entry.ObjectType);
				} else if (entry.Name.StartsWith("[source]:")) {
					var key = entry.Name.Substring(0, 9);
					sourceMeta[key] = info.GetValue(entry.Name, entry.ObjectType);
				}
			}

			TimeStamp = (DateTimeOffset) info.GetValue("TimeStamp", typeof(DateTimeOffset));
		}

		IEventSource IEvent.EventSource {
			get { return new EventMessageSource(this);}
			set { throw new NotSupportedException(); }
		}

		public DateTimeOffset TimeStamp { get; private set; }

		IDictionary<string, object> IEvent.EventData {
			get { return eventMeta; }
		}

		void ISerializable.GetObjectData(SerializationInfo info, StreamingContext context) {
			foreach (var pair in sourceMeta) {
				var key = String.Format("[source]:{0}", pair.Key);
				info.AddValue(key, pair.Value);
			}

			foreach (var pair in eventMeta) {
				var key = String.Format("[event]:{0}", pair.Key);
				info.AddValue(key, pair.Value);
			}

			info.AddValue("TimeStamp", TimeStamp);
		}

		#region EventMessageSource

		class EventMessageSource : IEventSource {
			private readonly EventMessage message;

			public EventMessageSource(EventMessage message) {
				this.message = message;
			}

			public IEventSource ParentSource {
				get { return null; }
			}

			public IEnumerable<KeyValuePair<string, object>> Metadata {
				get { return message.sourceMeta; }
			}
		}

		#endregion
	}
}
