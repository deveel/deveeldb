using System;
using System.Collections.Generic;

namespace Deveel.Data.Routines {
	public abstract class RoutineBuilder : IRoutine {
		private bool unboundedSeen;
		private RoutineName routineName;
		private readonly List<RoutineParameter> parameters = new List<RoutineParameter>();

		protected abstract RoutineType RoutineType { get; }

		RoutineType IRoutine.Type {
			get { return RoutineType; }
		}

		RoutineName IRoutine.Name {
			get { return routineName; }
		}

		RoutineParameter[] IRoutine.Parameters {
			get { return parameters.ToArray(); }
		}

		protected bool HasUnboundParameter {
			get { return unboundedSeen; }
		}

		protected void SetName(RoutineName name) {
			routineName = name;
		}

		protected void AddParameter(RoutineParameter parameter) {
			if (parameter.IsUnbounded && unboundedSeen)
				throw new InvalidOperationException();

			unboundedSeen = parameter.IsUnbounded;
			parameters.Add(parameter);
		}

		ExecuteResult IRoutine.Execute(ExecuteContext context) {
			throw new NotImplementedException();
		}
	}
}
