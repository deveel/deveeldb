using System;

namespace Deveel.Data.Mapping {
	[AttributeUsage(AttributeTargets.Class, Inherited = false)]
	public sealed class TableNameAttribute : Attribute {
		public TableNameAttribute(string name) 
			: this(name, null) {
		}

		public TableNameAttribute(string name, string schema) {
			if (String.IsNullOrEmpty(name))
				throw new ArgumentNullException("name");

			Name = name;
			Schema = schema;
		}

		public string Name { get; private set; }

		public string Schema { get; private set; }
	}
}
