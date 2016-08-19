using System;

namespace Deveel.Data {
	[AttributeUsage(AttributeTargets.Field)]
	class ErrorNameAttribute : Attribute {
		public ErrorNameAttribute(string name) {
			Name = name;
		}

		public string Name { get; private set; }
	}
}
