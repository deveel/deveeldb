using System;
using System.Reflection;

using Deveel.Data.Sql.Expressions;
using Deveel.Data.Sql.Types;

namespace Deveel.Data.Design {
	public sealed class TypeMemberMapInfo {
		internal TypeMemberMapInfo(MemberInfo member, string columnName, SqlType columnType, bool isNotNull, SqlExpression defaultExpression) {
			Member = member;
			ColumnName = columnName;
			ColumnType = columnType;
			DefaultExpression = defaultExpression;
			NotNull = isNotNull;
		}

		public MemberInfo Member { get; private set; }

		public string ColumnName { get; private set; }

		public SqlType ColumnType { get; private set; }

		public SqlExpression DefaultExpression { get; private set; }

		public bool NotNull { get; private set; }
	}
}
