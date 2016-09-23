using System;
using System.Reflection;

using Deveel.Data.Design.Configuration;
using Deveel.Data.Sql.Tables;

namespace Deveel.Data.Design.Conventions {
	public sealed class UniqueAttributeConvention : MemberAttributeConvention<PropertyInfo, UniqueAttribute> {
		protected override void Apply(PropertyInfo memberInfo, UniqueAttribute attribute, ModelConfiguration configuration) {
			var typeModel = configuration.Type(memberInfo.ReflectedType);
			if (typeModel.IsMemberOfAnyConstraint(ConstraintType.Unique, memberInfo.Name))
				return;

			var unique = typeModel.Constraint(attribute.ConstraintName, ConstraintType.Unique);
			if (unique.HasMember(memberInfo.Name))
				return;

			unique.AddMember(memberInfo.Name);
		}
	}
}
