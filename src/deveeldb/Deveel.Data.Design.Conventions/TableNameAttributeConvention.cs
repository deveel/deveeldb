using System;
using System.Text;

using Deveel.Data.Design.Configuration;

namespace Deveel.Data.Design.Conventions {
	public sealed class TableNameAttributeConvention : MemberAttributeConvention<Type, TableNameAttribute> {
		protected override void Apply(Type type, TableNameAttribute attribute, ModelConfiguration configuration) {
			var typeModel = configuration.Type(type);

			if (!String.IsNullOrEmpty(typeModel.TableName))
				return;

			var tableName = new StringBuilder();
			if (!String.IsNullOrEmpty(attribute.Schema))
				tableName.Append(attribute.Schema).Append(".");

			tableName.Append(attribute.Name);

			typeModel.TableName = tableName.ToString();
		}
	}
}
