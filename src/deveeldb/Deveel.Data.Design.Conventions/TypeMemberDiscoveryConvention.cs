using System;
using System.Reflection;

using Deveel.Data.Design.Configuration;

namespace Deveel.Data.Design.Conventions {
	public sealed class TypeMemberDiscoveryConvention : IConfigurationConvention {
		void IConfigurationConvention.Apply(MemberInfo memberInfo, ModelConfiguration configuration) {
			if (!(memberInfo is PropertyInfo) &&
				!(memberInfo is FieldInfo))
				return;

			if (memberInfo is PropertyInfo) {
				var propInfo = (PropertyInfo) memberInfo;
				if (!propInfo.CanRead ||
					!propInfo.CanWrite)
					return;
			}

			var typeModel = configuration.Type(memberInfo.ReflectedType);
			typeModel.IncludeMember(memberInfo.Name, false);
		}
	}
}
