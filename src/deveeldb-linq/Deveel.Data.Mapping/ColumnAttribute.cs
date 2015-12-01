using System;

using Deveel.Data.Sql;
using Deveel.Data.Types;

namespace Deveel.Data.Mapping {
	[AttributeUsage(AttributeTargets.Field | AttributeTargets.Property)]
	public sealed class ColumnAttribute : Attribute {
		public ColumnAttribute(string name, SqlTypeCode type, int size, int scale) {
			ColumnName = name;
			SqlType = type;
			Size = size;
			Scale = scale;
		}

		public ColumnAttribute(string name, SqlTypeCode type, int size)
			: this(name, type, size, -1) {
		}

		public ColumnAttribute(string name, SqlTypeCode type)
			: this(name, type, -1) {
		}

		public ColumnAttribute(string name)
			: this(name, SqlTypeCode.Unknown) {
		}

		public ColumnAttribute(SqlTypeCode type, int size, int scale)
			: this(null, type, size, scale) {
		}

		public ColumnAttribute(SqlTypeCode type, int size)
			: this(type, size, -1) {
		}

		public ColumnAttribute(SqlTypeCode type)
			: this(type, -1) {
		}

		public ColumnAttribute(int size, int scale)
			: this(null, SqlTypeCode.Unknown, size, scale) {
		}

		public ColumnAttribute(int size)
			: this(size, -1) {
		}

		public ColumnAttribute()
			: this(null, SqlTypeCode.Unknown, -1, -1) {
		}

		public string ColumnName { get; set; }

		public SqlTypeCode SqlType { get; set; }

		public int Size { get; set; }

		public int Scale { get; set; }
	}
}