using System;
using System.Reflection;

using Deveel.Data.Design.Configuration;
using Deveel.Data.Sql.Types;

namespace Deveel.Data.Design.Conventions {
	public sealed class ColumnAttributeConvention : MemberAttributeConvention<PropertyInfo, ColumnAttribute> {
		protected override void Apply(PropertyInfo memberInfo, ColumnAttribute attribute, ModelConfiguration configuration) {
			var typeModel = configuration.Type(memberInfo.ReflectedType);
			var memberModel = typeModel.GetMember(memberInfo.Name);
			if (memberModel == null)
				return;

			if (configuration.IsDependantMember(memberModel.TypeModel.Type, memberModel.Member.Name))
				return;

			if (String.IsNullOrEmpty(memberModel.ColumnName))
				memberModel.ColumnName = attribute.Name;

			if (memberModel.ColumnType == null) {
				if (!String.IsNullOrEmpty(attribute.TypeName)) {
					var typeName = attribute.TypeName;
					var meta = new[] {
						new DataTypeMeta("MaxSize", attribute.Size.ToString()),
						new DataTypeMeta("Precision", attribute.Precision.ToString()),
						new DataTypeMeta("Scale", attribute.Scale.ToString())
					};

					memberModel.ColumnType = PrimitiveTypes.Resolve(typeName, meta);
				} else if (!String.IsNullOrEmpty(attribute.Type)) {
					memberModel.ColumnType = SqlType.Parse(attribute.Type);
				}
			}
		}
	}
}
