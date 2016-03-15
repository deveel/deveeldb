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

using Deveel.Data.Sql;
using Deveel.Data.Transactions;

namespace Deveel.Data {
	static class UserSessionExtensions {
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
			session.Enter(new [] {obj}, accessType);
		}

		#endregion
	}
}