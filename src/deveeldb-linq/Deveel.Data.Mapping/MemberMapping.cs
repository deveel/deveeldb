using System;

using Deveel.Data.Types;

namespace Deveel.Data.Mapping {
	public sealed class MemberMapping {
		internal MemberMapping(TypeMapping typeMapping, string memberName, string columnName, SqlType columnType, bool notNull) {
			TypeMapping = typeMapping;
			MemberName = memberName;
			ColumnName = columnName;
			ColumnType = columnType;
			NotNull = notNull;
		}

		public TypeMapping TypeMapping { get; private set; }

		public string MemberName { get; private set; }

		public string ColumnName { get; private set; }

		public SqlType ColumnType { get; private set; }

		public bool NotNull { get; private set; }
	}
}
