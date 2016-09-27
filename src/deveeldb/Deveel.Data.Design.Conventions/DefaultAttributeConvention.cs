using System;
using System.Reflection;

using Deveel.Data.Design.Configuration;
using Deveel.Data.Sql;
using Deveel.Data.Sql.Expressions;

namespace Deveel.Data.Design.Conventions {
	public sealed class DefaultAttributeConvention : MemberAttributeConvention<PropertyInfo, DefaultAttribute> {
		protected override void Apply(PropertyInfo memberInfo, DefaultAttribute attribute, ModelConfiguration configuration) {
			var typeModel = configuration.Type(memberInfo.ReflectedType);
			var memberModel = typeModel.GetMember(memberInfo.Name);

			if (memberModel.DefaultExpression != null)
				return;

			if (attribute.DefaultType == ColumnDefaultType.Constant) {
				memberModel.DefaultExpression = SqlExpression.Constant(attribute.Value);
			} else {
				memberModel.DefaultExpression = SqlExpression.Parse((string) attribute.Value);
			}
		}
	}
}
