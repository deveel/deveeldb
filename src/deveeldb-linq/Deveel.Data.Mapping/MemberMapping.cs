using System;
using System.Reflection;

using Deveel.Data.Sql.Types;

namespace Deveel.Data.Mapping {
	public sealed class MemberMapping {
		internal MemberMapping(TypeMapping typeMapping, MemberInfo member, string columnName, SqlType columnType, bool notNull, bool primaryKey, bool unique, bool uniqueKey) {
			TypeMapping = typeMapping;
			Member = member;
			ColumnName = columnName;
			ColumnType = columnType;
			NotNull = notNull;
			PrimaryKey = primaryKey;
			Unique = unique;
			UniqueKey = uniqueKey;
		}

		public MemberInfo Member { get; private set; }

		public TypeMapping TypeMapping { get; private set; }

		public string MemberName {
			get { return Member.Name; }
		}

		public string ColumnName { get; private set; }

		public SqlType ColumnType { get; private set; }

		public bool NotNull { get; private set; }

		public bool PrimaryKey { get; private set; }

		public bool Unique { get; private set; }

		public bool UniqueKey { get; private set; }
	}
}
