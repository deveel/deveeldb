using System;

using Deveel.Data.DbSystem;
using Deveel.Data.Sql;
using Deveel.Data.Sql.Statements;

namespace Deveel.Data.Diagnostics {
	public static class QueryContextEventExtensions {
		public static void RegisterEvent(this IQueryContext context, IEvent @event) {
			context.DatabaseContext.SystemContext.EventRegistry.RegisterEvent(@event);
		}

		public static void RegisterError(this IQueryContext context, string message, Exception error) {
			var errorEx = new ErrorException(EventClasses.Runtime, Int32.MinValue, message, error);
			var errorEvent = errorEx.AsEvent(context.Session);
			context.RegisterEvent(errorEvent);
		}

		public static void RegisterError(this IQueryContext context, ErrorException error) {
			var errorEvent = error.AsEvent(context.Session);
			context.RegisterEvent(errorEvent);
		}

		public static void RegisterError(this IQueryContext context, int eventClass, int errorCode, string message) {
			context.RegisterError(new ErrorException(eventClass, errorCode, message));
		}

		public static void RegisterQuery(this IQueryContext context, SqlQuery query, string statementText) {
			
		}

		public static void RegisterQuery(this IQueryContext context, SqlStatement statement) {
			if (statement == null)
				throw new ArgumentNullException("statement");

			context.RegisterQuery(statement.SourceQuery, statement.ToString());
		}
	}
}
