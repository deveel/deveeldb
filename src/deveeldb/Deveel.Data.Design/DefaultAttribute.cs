using System;

namespace Deveel.Data.Design {
	[AttributeUsage(AttributeTargets.Field | AttributeTargets.Property)]
	public class DefaultAttribute : Attribute {
		public DefaultAttribute(object value) 
			: this(value, ColumnDefaultType.Constant) {
		}

		public DefaultAttribute(object value, ColumnDefaultType defaultType) {
			if (defaultType == ColumnDefaultType.Expression) {
				if (value == null)
					throw new ArgumentNullException("value");

				if (!(value is string))
					throw new ArgumentException("An expression default value must always be a valid SQL Expression string.");
			}

			Value = value;
			DefaultType = defaultType;
		}

		public object Value { get; private set; }

		public ColumnDefaultType DefaultType { get; set; }
	}
}
