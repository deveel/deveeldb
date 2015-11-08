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

using Deveel.Data;
using Deveel.Data.Sql;
using Deveel.Data.Sql.Statements;
using Deveel.Data.Sql.Triggers;

namespace Deveel.Data.Diagnostics {
	public static class QueryContextEventExtensions {
		public static void RegisterEvent(this IQueryContext context, IEvent @event) {
			context.SystemContext().EventRegistry.RegisterEvent(@event);
		}

		public static void RegisterError(this IQueryContext context, string message) {
			RegisterError(context, message, null);
		}

		public static void RegisterError(this IQueryContext context, string message, Exception error) {
			var errorEx = new ErrorException(EventClasses.Runtime, Int32.MinValue, message, error);
			var errorEvent = errorEx.AsEvent(context.Session());
			context.RegisterEvent(errorEvent);
		}

		public static void RegisterError(this IQueryContext context, ErrorException error) {
			var errorEvent = error.AsEvent(context.Session());
			context.RegisterEvent(errorEvent);
		}

		public static void RegisterError(this IQueryContext context, int eventClass, int errorCode, string message) {
			context.RegisterError(new ErrorException(eventClass, errorCode, message));
		}

		public static void RegisterQuery(this IQueryContext context, SqlQuery query, string statementText) {
			context.RegisterEvent(new QueryEvent(context.Session(), query, statementText));
		}

		public static void RegisterQuery(this IQueryContext context, SqlStatement statement) {
			if (statement == null)
				throw new ArgumentNullException("statement");

			context.RegisterQuery(statement.SourceQuery, statement.ToString());
		}
	}
}
