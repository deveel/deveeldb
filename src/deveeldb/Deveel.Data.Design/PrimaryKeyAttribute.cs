using System;

namespace Deveel.Data.Design {
	[AttributeUsage(AttributeTargets.Field | AttributeTargets.Property, AllowMultiple = false)]
	public sealed class PrimaryKeyAttribute : ColumnConstraintAttribute {
		public PrimaryKeyAttribute()
			: base(ColumnConstraintType.PrimaryKey) {
		}
	}
}
