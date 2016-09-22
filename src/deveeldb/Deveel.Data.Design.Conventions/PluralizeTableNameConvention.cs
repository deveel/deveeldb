using System;
using System.Reflection;

using Deveel.Data.Design.Configuration;

namespace Deveel.Data.Design.Conventions {
	public sealed class PluralizeTableNameConvention : IConfigurationConvention {
		void IConfigurationConvention.Apply(MemberInfo memberInfo, ModelConfiguration configuration) {
			if (!(memberInfo is Type))
				return;

			var type = (Type)memberInfo;
			var typeModel = configuration.Type(type);

			if (!String.IsNullOrEmpty(typeModel.TableName))
				return;

			// TODO:
		}
	}
}
