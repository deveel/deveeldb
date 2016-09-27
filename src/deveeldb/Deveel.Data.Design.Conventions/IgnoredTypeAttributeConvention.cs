using System;

using Deveel.Data.Design.Configuration;

namespace Deveel.Data.Design.Conventions {
	public sealed class IgnoredTypeAttributeConvention : MemberAttributeConvention<Type, IgnoreAttribute> {
		protected override void Apply(Type type, IgnoreAttribute attribute, ModelConfiguration configuration) {
			configuration.IgnoreType(type);
		}
	}
}
