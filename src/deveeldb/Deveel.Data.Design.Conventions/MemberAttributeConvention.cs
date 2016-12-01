using System;
using System.Linq;
using System.Reflection;

using Deveel.Data.Design.Configuration;

namespace Deveel.Data.Design.Conventions {
	public abstract class MemberAttributeConvention<TMemberInfo, TAttribute> : IConfigurationConvention 
		where TMemberInfo : MemberInfo
		where TAttribute : Attribute {
		protected abstract void Apply(TMemberInfo memberInfo, TAttribute attribute, ModelConfiguration configuration);

		void IConfigurationConvention.Apply(MemberInfo member, ModelConfiguration configuration) {
			if (!(member is TMemberInfo))
				return;

			var attributes = member.GetCustomAttributes(true).OfType<TAttribute>();
			foreach (var attribute in attributes) {
				Apply((TMemberInfo)member, attribute, configuration);
			}
		}
	}
}
