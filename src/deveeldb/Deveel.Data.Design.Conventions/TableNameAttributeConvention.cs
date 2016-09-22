using System;
using System.Linq;
using System.Reflection;
using System.Text;

using Deveel.Data.Design.Configuration;

namespace Deveel.Data.Design.Conventions {
	public sealed class TableNameAttributeConvention : IConfigurationConvention {
		void IConfigurationConvention.Apply(MemberInfo memberInfo, ModelConfiguration configuration) {
			if (!(memberInfo is Type))
				return;

			var type = (Type) memberInfo;
			var typeModel = configuration.Type(type);

			if (!String.IsNullOrEmpty(typeModel.TableName))
				return;

			var attributes = memberInfo.GetCustomAttributes(typeof(TableNameAttribute), false);
			if (!attributes.Any())
				return;

			var attribute = (TableNameAttribute) attributes[0];

			var tableName = new StringBuilder();
			if (!String.IsNullOrEmpty(attribute.Schema))
				tableName.Append(attribute.Schema).Append(".");

			tableName.Append(attribute.Name);

			typeModel.TableName = tableName.ToString();
		}
	}
}
