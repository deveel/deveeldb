using System;
using System.Reflection;

using Deveel.Data.Design.Configuration;
using Deveel.Data.Sql.Tables;

namespace Deveel.Data.Design.Conventions {
	public sealed class PrimaryKeyAttributeConvention : MemberAttributeConvention<PropertyInfo, PrimaryKeyAttribute> {
		protected override void Apply(PropertyInfo memberInfo, PrimaryKeyAttribute attribute, ModelConfiguration configuration) {
			var type = memberInfo.ReflectedType;
			var typeModel = configuration.Type(type);

			if (typeModel.HasPrimaryKey)
				return;

			var constraint = typeModel.Constraint(ConstraintType.PrimaryKey);
			constraint.AddMember(memberInfo.Name);
		}
	}
}
