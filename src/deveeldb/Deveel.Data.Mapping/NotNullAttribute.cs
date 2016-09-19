using System;

namespace Deveel.Data.Mapping {
	[AttributeUsage(AttributeTargets.Field | AttributeTargets.Property, AllowMultiple = false)]
	public sealed class NotNullAttribute : ColumnConstraintAttribute {
		public NotNullAttribute()
			: base(ColumnConstraintType.NotNull) {
		}
	}
}
