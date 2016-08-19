using System;
using System.Reflection;

using Deveel.Data.Sql.Types;

namespace Deveel.Data.Linq {
	public sealed class DbColumnModel : IDbModel {
		internal DbColumnModel(MemberInfo member, string columnName, SqlType columnType) {
			Member = member;
			ColumnName = columnName;
			ColumnType = columnType;
		}

		public MemberInfo Member { get; private set; }

		public string ColumnName { get; private set; }

		public bool IsKey { get; internal set; }

		public KeyType KeyType { get; internal set; }

		public SqlType ColumnType { get; private set; }

		public bool IsNullable { get; internal set; }
	}
}
