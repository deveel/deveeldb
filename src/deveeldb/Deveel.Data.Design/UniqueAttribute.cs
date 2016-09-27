using System;

namespace Deveel.Data.Design {
	[AttributeUsage(AttributeTargets.Field | AttributeTargets.Property, AllowMultiple = false)]
	public sealed class UniqueAttribute : ColumnConstraintAttribute {
		public UniqueAttribute()
			: base(ColumnConstraintType.Unique) {
		}

		public string ConstraintName { get; set; }
	}
}
