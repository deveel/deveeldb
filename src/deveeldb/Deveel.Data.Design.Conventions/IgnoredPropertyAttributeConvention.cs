using System;
using System.Reflection;

using Deveel.Data.Design.Configuration;

namespace Deveel.Data.Design.Conventions {
	public sealed class IgnoredPropertyAttributeConvention : MemberAttributeConvention<PropertyInfo, IgnoreAttribute> {
		protected override void Apply(PropertyInfo memberInfo, IgnoreAttribute attribute, ModelConfiguration configuration) {
			var typeModel = configuration.Type(memberInfo.ReflectedType);
			typeModel.IgnoreMember(memberInfo.Name);
		}
	}
}
