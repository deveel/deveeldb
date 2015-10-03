using System;
using System.Reflection;

namespace Deveel.Data.Mapping {
	public sealed class RelationshipConfiguration<T> where T : class {
		private readonly MemberInfo memberInfo;

		internal RelationshipConfiguration(MemberInfo memberInfo) {
			this.memberInfo = memberInfo;
		}
	}
}
