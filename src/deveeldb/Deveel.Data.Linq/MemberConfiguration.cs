using System;
using System.Reflection;

namespace Deveel.Data.Linq {
	public sealed class MemberConfiguration {
		internal MemberConfiguration(MemberInfo member) {
			Member = member;
		}

		internal MemberInfo Member { get; private set; }

		internal string ColumnName { get; private set; }

		internal int? ColumnSize { get; private set; }

		internal bool MaxSize { get; private set; }

		internal bool IsNullable { get; private set; }

		public MemberConfiguration HasColumnName(string columnName) {
			ColumnName = columnName;
			return this;
		}

		public MemberConfiguration HasSize(int value) {
			ColumnSize = value;
			return this;
		}

		public MemberConfiguration IsMaxSize(bool value = true) {
			MaxSize = value;
			return this;
		}

		public MemberConfiguration Nullable(bool value = true) {
			IsNullable = value;
			return this;
		}
	}
}
