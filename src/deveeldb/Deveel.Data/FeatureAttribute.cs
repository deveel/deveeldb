using System;

namespace Deveel.Data {
	[AttributeUsage(AttributeTargets.Class)]
	public sealed class FeatureAttribute : Attribute {
		public FeatureAttribute() : this(null) {
		}

		public FeatureAttribute(string name) : this(name, null) {
		}

		public FeatureAttribute(string name, string version) {
			Name = name;
			Version = version;
		}

		public string Name { get; set; }

		public string Version { get; set; }
	}
}
