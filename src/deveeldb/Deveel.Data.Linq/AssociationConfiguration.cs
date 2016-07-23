using System;
using System.Reflection;

namespace Deveel.Data.Linq {
	public sealed class AssociationConfiguration<T> {
		internal AssociationConfiguration(MemberInfo member) {
			Member = member;
		}

		internal MemberInfo Member { get; private set; }
	}
}
