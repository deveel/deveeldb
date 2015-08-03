using System;
using System.Collections.Generic;

using Deveel.Data.Routines;

namespace Deveel.Data.Sql.Statements {
	public interface IRoutineStatement : IProgramSatement {
		IEnumerable<RoutineParameter> Parameters { get; } 
			
		IEnumerable<IDeclarationStatement> Declarations { get; }
	}
}
