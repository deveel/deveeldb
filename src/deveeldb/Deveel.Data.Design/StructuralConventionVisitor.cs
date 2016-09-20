using System;

namespace Deveel.Data.Design {
	public class StructuralConventionVisitor {
		public virtual void Visit(TypeBuildInfo typeInfo, IConvention convention) {
			if (convention is IStructuralConvention)
				VisitStructuralConvention(typeInfo, (IStructuralConvention) convention);
		}

		public virtual void VisitStructuralConvention(TypeBuildInfo typeInfo, IStructuralConvention convention) {
			
		}
	}
}
