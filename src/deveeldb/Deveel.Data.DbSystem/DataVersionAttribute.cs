using System;

namespace Deveel.Data.DbSystem {
	[AttributeUsage(AttributeTargets.Assembly)]
	class DataVersionAttribute : Attribute {
		public DataVersionAttribute(string version) {
			Version = new Version(version);
		}

		public Version Version { get; private set; }
	}
}
