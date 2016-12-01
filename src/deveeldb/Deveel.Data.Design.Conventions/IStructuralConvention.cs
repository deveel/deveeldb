using System;

namespace Deveel.Data.Design.Conventions {
	public interface IStructuralConvention : IConvention {
		void Apply(DbModel model);
	}
}
