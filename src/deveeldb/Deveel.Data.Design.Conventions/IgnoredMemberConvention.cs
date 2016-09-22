using System;
using System.Reflection;

using Deveel.Data.Design.Configuration;

namespace Deveel.Data.Design.Conventions {
	public sealed class IgnoredMemberConvention : IConfigurationConvention {
		void IConfigurationConvention.Apply(MemberInfo memberInfo, ModelConfiguration configuration) {

		}
	}
}
