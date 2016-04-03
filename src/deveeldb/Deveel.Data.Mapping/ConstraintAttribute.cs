using System;

using Deveel.Data.Sql.Tables;

namespace Deveel.Data.Mapping {
	[AttributeUsage(AttributeTargets.Field | AttributeTargets.Property)]
	public sealed class ConstraintAttribute : Attribute {
		public ConstraintAttribute(ConstraintType type) {
			if (type == ConstraintType.ForeignKey)
				throw new NotImplementedException();

			Type = type;
		}

		public ConstraintType Type { get; private set; }

		public string Expression { get; set; }
	}
}
