using System;

namespace Deveel.Data.Design {
	public interface IStructuralConvention : IConvention {
		void Apply(TypeBuildInfo typeInfo);

		void Apply(TypeBuildMemberInfo memberInfo);
	}
}
