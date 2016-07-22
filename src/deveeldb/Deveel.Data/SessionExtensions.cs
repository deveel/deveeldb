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
using System.Globalization;
using System.Linq;

using Deveel.Data.Diagnostics;
using Deveel.Data.Sql;
using Deveel.Data.Sql.Types;
using Deveel.Data.Transactions;

namespace Deveel.Data {
	public static class SessionExtensions {
		public static IDatabase Database(this ISession session) {
			return session.Transaction.Database;
		}

		internal static SystemAccess Access(this ISession session) {
			if (!(session is IProvidesDirectAccess))
				throw new InvalidOperationException("The session does not provide direct access to the system.");

			return ((IProvidesDirectAccess) session).DirectAccess;
		}

		public static bool IsFinished(this ISession session) {
			return session.Transaction == null ||
			       session.Transaction.State == TransactionState.Finished;
		}

		public static IEventSource AsEventSource(this ISession session) {
			if (session == null)
				throw new ArgumentNullException("session");

			var source = session as IEventSource;
			if (source == null)
				source = new EventSource(session.Context, session.Transaction.AsEventSource());

			return source;
		}

		// TODO: In a future version of deveeldb the transaction will be a child of
		//       the session's wrapped one
		public static ISession Begin(this ISession session, IsolationLevel isolation) {
			var transaction = session.Database().TransactionFactory.CreateTransaction(isolation);
			return new Session(transaction, session.User.Name);
		}

		public static void Enter(this ISession session, IEnumerable<IDbObject> objects, AccessType accessType) {
			// If a transaction is null, no reference can be acquired anymore
			if (session.Transaction != null)
				session.Transaction.Enter(objects, accessType);
		}

		public static void Exit(this ISession session, IEnumerable<IDbObject> objects, AccessType accessType) {
			// If a transaction is null, all references have already been released
			if (session.Transaction != null)
				session.Transaction.Exit(objects, accessType);
		}

		#region Variables

		public static bool AutoCommit(this ISession session) {
			return session.Transaction.AutoCommit();
		}

		public static void AutoCommit(this ISession session, bool value) {
			session.Transaction.AutoCommit(value);
		}

		public static void CurrentSchema(this ISession session, string value) {
			session.Transaction.CurrentSchema(value);
		}

		public static string CurrentSchema(this ISession session) {
			return session.Transaction.CurrentSchema();
		}

		public static bool IgnoreIdentifiersCase(this ISession session) {
			return session.Transaction.IgnoreIdentifiersCase();
		}

		public static void IgnoreIdentifiersCase(this ISession session, bool value) {
			session.Transaction.IgnoreIdentifiersCase(value);
		}

		public static QueryParameterStyle ParameterStyle(this ISession session) {
			return session.Transaction.ParameterStyle();
		}

		public static void ParameterStyle(this ISession session, QueryParameterStyle value) {
			session.Transaction.ParameterStyle(value);
		}

		#endregion

		#region Locks

		public static void Enter(this ISession session, IDbObject obj, AccessType accessType) {
			session.Enter(new[] { obj }, accessType);
		}

		#endregion

		#region Metadata

		private static T GetMeta<T>(this ISession session, string key) {
			var eventSource = session.AsEventSource();
			if (eventSource == null ||
				eventSource.Metadata == null)
				return default(T);

			var dict = eventSource.Metadata.ToDictionary(x => x.Key, y => y.Value);
			object value;
			if (!dict.TryGetValue(key, out value))
				return default(T);

			if (value is T)
				return (T) value;

#if !PCL
			if (value is IConvertible)
				return (T) Convert.ChangeType(value, typeof (T), CultureInfo.InvariantCulture);

			throw new InvalidCastException();
#else
			return (T) Convert.ChangeType(value, typeof (T), CultureInfo.InvariantCulture);
#endif
		}

		private static bool HasMeta(this ISession session, string key) {
			var eventSource = session.AsEventSource();
			if (eventSource == null)
				return false;

			return eventSource.Metadata != null && eventSource.Metadata.Any(x => x.Key == key);
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
