using System;
using System.Linq;
using System.Reflection;

using Deveel.Data.Design.Configuration;
using Deveel.Data.Sql.Tables;

namespace Deveel.Data.Design.Conventions {
	public sealed class PrimaryKeyDiscoveryConvention : IConfigurationConvention {
		void IConfigurationConvention.Apply(MemberInfo memberInfo, ModelConfiguration configuration) {
			if (memberInfo is Type)
				return;

			var type = memberInfo.ReflectedType;
			var typeModel = configuration.Type(type);

			if (typeModel.HasPrimaryKey)
				return;

			string idMember;

			if (String.Equals(memberInfo.Name, "Id", StringComparison.OrdinalIgnoreCase)) {
				idMember = memberInfo.Name;
			} else {
				idMember = typeModel.MemberNames
					.FirstOrDefault(x => x.Equals(String.Format("{0}_Id", type.Name), StringComparison.Ordinal));
				if (idMember == null)
					idMember = typeModel.MemberNames
						.FirstOrDefault(x => x.Equals(String.Format("{0}Id", type.Name)));
			}

			if (idMember != null) {
				var key = typeModel.Constraint(ConstraintType.PrimaryKey);
				key.AddMember(idMember);
			}
		}
	}
}
