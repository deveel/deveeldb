using System;
using System.Reflection;

using Deveel.Data.Design.Configuration;

namespace Deveel.Data.Design.Conventions {
	public interface IConfigurationConvention : IConvention {
		void Apply(MemberInfo member, ModelConfiguration configuration);
	}
}
