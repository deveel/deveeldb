using System;
using System.Globalization;

namespace Deveel.Data.Diagnostics {
	public static class EventExtensions {
		public static T GetData<T>(this IEvent @event, string key) {
			if (@event == null || @event.EventData == null)
				return default(T);

			object value;
			if (!@event.EventData.TryGetValue(key, out value))
				return default(T);

			if (value is T)
				return (T) value;

			if (value is IConvertible)
				return (T) Convert.ChangeType(value, typeof (T), CultureInfo.InvariantCulture);

			throw new InvalidCastException();
		}

		public static string UserName(this IEvent @event) {
			return @event.GetData<string>(KnownEventMetadata.UserName);
		}

		public static string DatabaseName(this IEvent @event) {
			return @event.GetData<string>(KnownEventMetadata.DatabaseName);
		}

		public static int CommitId(this IEvent @event) {
			return @event.GetData<int>(KnownEventMetadata.CommitId);
		}

		public static DateTimeOffset SessionStartTime(this IEvent @event) {
			return @event.GetData<DateTimeOffset>(KnownEventMetadata.SessionStartTime);
		}
	}
}
