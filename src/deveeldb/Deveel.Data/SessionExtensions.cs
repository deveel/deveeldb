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
using System.Globalization;
using System.Linq;

using Deveel.Data.Transactions;

namespace Deveel.Data {
	public static class SessionExtensions {
		public static IDatabase Database(this ISession session) {
			return session.Transaction.Database;
		}

		// TODO: In a future version of deveeldb the transaction will be a child of
		//       the session's wrapped one
		public static ISession Begin(this ISession session, IsolationLevel isolation) {
			var transaction = session.Database().TransactionFactory.CreateTransaction(isolation);
			return new Session(transaction, session.User.Name);
		}

		#region Metadata

		private static T GetMeta<T>(this ISession session, string key) {
			if (session.Metadata == null)
				return default(T);

			var dict = session.Metadata.ToDictionary(x => x.Key, y => y.Value);
			object value;
			if (!dict.TryGetValue(key, out value))
				return default(T);

			if (value is T)
				return (T) value;

			if (value is IConvertible)
				return (T) Convert.ChangeType(value, typeof (T), CultureInfo.InvariantCulture);

			throw new InvalidCastException();
		}

		private static bool HasMeta(this ISession session, string key) {
			return session.Metadata != null && session.Metadata.Any(x => x.Key == key);
		}

		public static bool HasCommandTime(this ISession session) {
			return session.HasMeta("lastCommandTime");
		}

		public static DateTimeOffset LastCommandTime(this ISession session) {
			return session.GetMeta<DateTimeOffset>("lastCommandTime");
		}

		public static DateTimeOffset StartedOn(this ISession session) {
			return session.HasMeta("startTime") ? session.GetMeta<DateTimeOffset>("startTime") : DateTimeOffset.MinValue;
		}

		public static TimeSpan TimeZoneOffset(this ISession session) {
			if (!session.HasMeta("timeZone.hours") ||
				!session.HasMeta("timeZone.minutes"))
				return TimeSpan.Zero;

			var hours = session.GetMeta<int>("timeZone.hours");
			var minutes = session.GetMeta<int>("timeZone.minutes");
			return new TimeSpan(0, hours, minutes, 0);
		}

		#endregion
	}
}
