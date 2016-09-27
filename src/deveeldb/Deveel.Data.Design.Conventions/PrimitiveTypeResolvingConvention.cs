using System;
using System.Reflection;

using Deveel.Data.Design.Configuration;
using Deveel.Data.Sql.Types;

namespace Deveel.Data.Design.Conventions {
	public sealed class PrimitiveTypeResolvingConvention : IConfigurationConvention {
		void IConfigurationConvention.Apply(MemberInfo memberInfo, ModelConfiguration configuration) {
			if (memberInfo is Type)
				return;

			var type = memberInfo.ReflectedType;
			var typeModel = configuration.Type(type);

			var memberModel = typeModel.GetMember(memberInfo.Name);
			if (memberModel != null) {
				if (memberModel.ColumnType != null)
					return;

				Type memberType;
				if (memberInfo is PropertyInfo) {
					memberType = ((PropertyInfo) memberInfo).PropertyType;
				} else if (memberInfo is FieldInfo) {
					memberType = ((FieldInfo) memberInfo).FieldType;
				} else {
					throw new InvalidOperationException();
				}

				if (PrimitiveTypes.IsPrimitive(memberType))
					memberModel.ColumnType = PrimitiveTypes.FromType(memberType);
			}
		}
	}
}
