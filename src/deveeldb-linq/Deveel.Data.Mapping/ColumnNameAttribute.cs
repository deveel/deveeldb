using System;

namespace Deveel.Data.Mapping {
	[AttributeUsage(AttributeTargets.Field | AttributeTargets.Property)]
	public sealed class ColumnNameAttribute : Attribute {
		public ColumnNameAttribute() 
			: this(null) {
		}

		public ColumnNameAttribute(string columnName) {
			ColumnName = columnName;
		}

		public string ColumnName { get; set; }
	}
}
