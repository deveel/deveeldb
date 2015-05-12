using System;
using System.Globalization;

namespace Deveel.Data.Diagnostics {
	public static class EventExtensions {
		public static void SetData(this IEvent @event, string key, object value) {
			if (@event.EventData == null)
				return;

			if (String.IsNullOrEmpty(key))
				throw new ArgumentNullException("key");

			@event.EventData[key] = value;
		}

		public static T GetData<T>(this IEvent @event, string key) {
			if (String.IsNullOrEmpty(key))
				throw new ArgumentNullException("key");

			if (@event.EventData == null)
				return default(T);

			object obj;
			if (!@event.EventData.TryGetValue(key, out obj))
				return default(T);

			if (!(obj is T) &&
			    obj is IConvertible)
				obj = Convert.ChangeType(obj, typeof (T), CultureInfo.InvariantCulture);

			return (T) obj;
		}

		public static void Database(this IEvent @event, string value) {
			@event.SetData(EventMetadataKeys.Database, value);
		}

		public static string Database(this IEvent @event) {
			return @event.GetData<string>(EventMetadataKeys.Database);
		}

		public static void UserName(this IEvent @event, string value) {
			@event.SetData(EventMetadataKeys.UserName, value);
		}

		public static string UserName(this IEvent @event) {
			return @event.GetData<string>(EventMetadataKeys.UserName);
		}

		public static void StackTrace(this IEvent @event, string value) {
			@event.SetData(EventMetadataKeys.StackTrace, value);
		}

		public static string StackTrace(this IEvent @event) {
			return @event.GetData<string>(EventMetadataKeys.StackTrace);
		}

		public static void ErrorSource(this IEvent @event, string value) {
			@event.SetData(EventMetadataKeys.Source, value);
		}

		public static string ErrorSource(this IEvent @event) {
			return @event.GetData<string>(EventMetadataKeys.Source);
		}

		public static void ErrorLevel(this IEvent @event, ErrorLevel value) {
			@event.SetData(EventMetadataKeys.ErrorLevel, value);
		}

		public static ErrorLevel ErrorLevel(this IEvent @event) {
			return @event.GetData<ErrorLevel>(EventMetadataKeys.ErrorLevel);
		}
	}
}
