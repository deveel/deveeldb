using System;

namespace Deveel.Data.Mapping {
	[AttributeUsage(AttributeTargets.Field | AttributeTargets.Property, AllowMultiple = true)]
	public class ColumnConstraintAttribute : Attribute {
		public ColumnConstraintAttribute(ColumnConstraints constraints) {
			this.Constraints = constraints;
		}

		public ColumnConstraints Constraints { get; private set; }
	}
}