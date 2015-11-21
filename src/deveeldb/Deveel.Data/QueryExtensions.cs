using System;

using Deveel.Data.Security;
using Deveel.Data.Sql;

namespace Deveel.Data {
	public static class QueryExtensions {
		public static User User(this IQuery query) {
			return query.Session.User;
		}

		public static string UserName(this IQuery query) {
			return query.User().Name;
		}

		internal static IQuery Direct(this IQuery query) {
			var systemSession = new SystemUserSession(query.Session.Transaction, query.Session.CurrentSchema);
			return new Query(systemSession);
		}

		public static bool IgnoreIdentifiersCase(this IQuery query) {
			return query.QueryContext.IgnoreIdentifiersCase();
		}

		public static void IgnoreIdentifiersCase(this IQuery query, bool value) {
			query.QueryContext.IgnoreIdentifiersCase(value);
		}

		public static void AutoCommit(this IQuery query, bool value) {
			query.QueryContext.AutoCommit(value);
		}

		public static bool AutoCommit(this IQuery query) {
			return query.QueryContext.AutoCommit();
		}

		public static string CurrentSchema(this IQuery query) {
			return query.QueryContext.CurrentSchema();
		}

		public static void CurrentSchema(this IQuery query, string value) {
			query.QueryContext.CurrentSchema(value);
		}

		public static void ParameterStyle(this IQuery query, QueryParameterStyle value) {
			query.QueryContext.ParameterStyle(value);
		}

		public static QueryParameterStyle ParameterStyle(this IQuery query) {
			return query.QueryContext.ParameterStyle();
		}

		public static void Commit(IQuery query) {
			query.Session.Commit();
		}

		public static void Rollback(this IQuery query) {
			query.Session.Rollback();
		}
	}
}
