using System;

namespace Deveel.Data.Linq {
	[AttributeUsage(AttributeTargets.Property | AttributeTargets.Field, AllowMultiple = false)]
	public sealed class ColumnAttribute : Attribute {
		public ColumnAttribute() 
			: this(null) {
		}

		public ColumnAttribute(string columnName) {
			ColumnName = columnName;
		}

		public string ColumnName { get; set; }
	}
}
