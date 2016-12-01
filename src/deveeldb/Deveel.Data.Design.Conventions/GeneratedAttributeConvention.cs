using System;
using System.Reflection;

using Deveel.Data.Design.Configuration;

namespace Deveel.Data.Design.Conventions {
	public sealed class GeneratedAttributeConvention : MemberAttributeConvention<PropertyInfo, GeneratedAttribute> {
		protected override void Apply(PropertyInfo memberInfo, GeneratedAttribute attribute, ModelConfiguration configuration) {
			var typeInfo = configuration.Type(memberInfo.ReflectedType);
			var member = typeInfo.GetMember(memberInfo.Name);

			if (member.Generated)
				return;

			// TODO: Get also the kind of generation this is ...

			member.Generated = true;
		}
	}
}
