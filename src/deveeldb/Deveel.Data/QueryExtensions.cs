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
			var systemSession = new SystemSession(query.Session.Transaction, query.Session.CurrentSchema);
			return new Query(systemSession);
		}

		public static bool IgnoreIdentifiersCase(this IQuery query) {
			return query.Context.IgnoreIdentifiersCase();
		}

		public static void IgnoreIdentifiersCase(this IQuery query, bool value) {
			query.Context.IgnoreIdentifiersCase(value);
		}

		public static void AutoCommit(this IQuery query, bool value) {
			query.Context.AutoCommit(value);
		}

		public static bool AutoCommit(this IQuery query) {
			return query.Context.AutoCommit();
		}

		public static string CurrentSchema(this IQuery query) {
			return query.Context.CurrentSchema();
		}

		public static void CurrentSchema(this IQuery query, string value) {
			query.Context.CurrentSchema(value);
		}

		public static void ParameterStyle(this IQuery query, QueryParameterStyle value) {
			query.Context.ParameterStyle(value);
		}

		public static QueryParameterStyle ParameterStyle(this IQuery query) {
			return query.Context.ParameterStyle();
		}

		public static void Commit(this IQuery query) {
			query.Session.Commit();
		}

		public static void Rollback(this IQuery query) {
			query.Session.Rollback();
		}
	}
}
