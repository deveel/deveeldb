using System;
using System.Globalization;

namespace Deveel.Data.Diagnostics {
	/// <summary>
	/// Extending methods for <see cref="IEvent"/> objects
	/// </summary>
	/// <seealso cref="IEvent"/>
	public static class EventExtensions {
		/// <summary>
		/// Sets a meta-data value for a given key
		/// </summary>
		/// <param name="event">The event containing the data.</param>
		/// <param name="key">The key of the value to set.</param>
		/// <param name="value">The value to set.</param>
		/// <exception cref="ArgumentNullException">
		/// If the <paramref name="key"/> specified is <c>null</c>.
		/// </exception>
		public static void SetData(this IEvent @event, string key, object value) {
			if (@event.EventData == null)
				return;

			if (String.IsNullOrEmpty(key))
				throw new ArgumentNullException("key");

			@event.EventData[key] = value;
		}

		/// <summary>
		/// Gets a meta-data value from the event.
		/// </summary>
		/// <typeparam name="T">The type of the returned value.</typeparam>
		/// <param name="event">The event containing the data.</param>
		/// <param name="key">The key of the value to get.</param>
		/// <returns>
		/// Returns a value of type <see cref="T"/> that was found for the
		/// specified <paramref name="key"/> or the default value of <see cref="T"/>.
		/// </returns>
		/// <exception cref="ArgumentNullException">
		/// If the <paramref name="key"/> specified is <c>null</c>.
		/// </exception>
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

		/// <summary>
		/// Sets the database name to the meta-data of the event.
		/// </summary>
		/// <param name="event">The event containing the data.</param>
		/// <param name="value">The name of the database.</param>
		public static void Database(this IEvent @event, string value) {
			@event.SetData(EventMetadataKeys.Database, value);
		}

		/// <summary>
		/// Gets the database name from the event meta-data.
		/// </summary>
		/// <param name="event">The event containing the data.</param>
		/// <returns></returns>
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

		public static void RemoteAddress(this IEvent @event, string value) {
			@event.SetData(EventMetadataKeys.RemoteAddress, value);
		}

		public static string RemoteAddress(this IEvent @event) {
			return @event.GetData<string>(EventMetadataKeys.RemoteAddress);
		}
	}
}
