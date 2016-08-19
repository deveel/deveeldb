using System;
using System.Reflection;

namespace Deveel.Data.Linq {
	public sealed class KeyConfiguration {
		internal KeyConfiguration(MemberInfo member) {
			Member = member;
		}

		internal MemberInfo Member { get; private set; }

		internal KeyType KeyType { get; private set; }

		public KeyConfiguration OfType(KeyType type) {
			KeyType = type;
			return this;
		}
	}
}
