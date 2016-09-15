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

		public static IEventSource AsEventSource(this ISession session) {
			if (session == null)
				return null;
			if (session is IEventSource)
				return (IEventSource) session;

			return new EventSource(session.Transaction.AsEventSource());
		}

		public static bool IsFinished(this ISession session) {
			return session.Transaction == null ||
			       session.Transaction.State == TransactionState.Finished;
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

		public static void Enter(this ISession session, IDbObject obj, AccessType accessType) {
			session.Enter(new[] { obj }, accessType);
		}

		public static void Exit(this ISession session, IEnumerable<IDbObject> objects, AccessType accessType) {
			// If a transaction is null, all references have already been released
			if (session.Transaction != null)
				session.Transaction.Exit(objects, accessType);
		}

		public static void Exit(this ISession session, IDbObject obj, AccessType accessType) {
			session.Exit(new [] {obj}, accessType);
		}

		public static void Lock(this ISession session, IEnumerable<ObjectName> objectNames, AccessType accessType,
			LockingMode mode, int timeout) {
			if (session.Transaction != null)
				session.Transaction.Lock(objectNames, accessType, mode, timeout);
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

		#region Metadata

		public static DateTimeOffset? LastCommandTime(this ISession session) {
			return session.AsEventSource().SessionLastCommandTime();
		}

		public static DateTimeOffset? StartedOn(this ISession session) {
			return session.AsEventSource().SessionStartTimeUtc();
		}

		public static TimeSpan TimeZoneOffset(this ISession session) {
			var timeZone = session.AsEventSource().SessionTimeZone();
			if (String.IsNullOrEmpty(timeZone))
				return TimeSpan.Zero;

			TimeSpan result;
			if (!TimeSpan.TryParse(timeZone, out result)) {
				session.OnWarning(new Exception(String.Format("A session timezone was set but it is invalid: {0}", timeZone)));
				return TimeSpan.Zero;
			}

			return result;
		}

		#endregion
	}
}
