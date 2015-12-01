using System;

namespace Deveel.Data.Mapping {
	[AttributeUsage(AttributeTargets.Field | AttributeTargets.Property)]
	public sealed class NotNullAttribute : ColumnConstraintAttribute {
		public NotNullAttribute()
			: base(ColumnConstraints.NotNull) {
		}
	}
}