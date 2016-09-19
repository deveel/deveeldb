using System;
using System.Reflection;

using Deveel.Data.Sql.Expressions;
using Deveel.Data.Sql.Types;

namespace Deveel.Data.Mapping {
	public sealed class TypeMemberMapInfo {
		internal TypeMemberMapInfo(MemberInfo member, string columnName, SqlType columnType, SqlExpression defaultExpression) {
			Member = member;
			ColumnName = columnName;
			ColumnType = columnType;
			DefaultExpression = defaultExpression;
		}

		public MemberInfo Member { get; private set; }

		public string ColumnName { get; private set; }

		public SqlType ColumnType { get; private set; }

		public SqlExpression DefaultExpression { get; private set; }
	}
}
