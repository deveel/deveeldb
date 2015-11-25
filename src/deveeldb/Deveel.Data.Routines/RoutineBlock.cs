using System;
using System.Collections.Generic;

using Deveel.Data.Sql;
using Deveel.Data.Sql.Objects;
using Deveel.Data.Sql.Variables;

namespace Deveel.Data.Routines {
	public sealed class RoutineBlock : Block {
		public RoutineBlock(IQuery query, RoutineInfo routineInfo) 
			: base(query) {
			if (routineInfo == null)
				throw new ArgumentNullException("routineInfo");

			RoutineInfo = routineInfo;
			InjectParameters();
		}

		public RoutineInfo RoutineInfo { get; private set; }

		protected override IEnumerable<KeyValuePair<string, object>> GetEventMetadata() {
			var meta = new Dictionary<string, object> {
				{ "routine.name", RoutineInfo.RoutineName },
				{ "routine.type", RoutineInfo.RoutineType }
			};

			for (int i = 0; i < RoutineInfo.Parameters.Length; i++) {
				var param = RoutineInfo.Parameters[i];
				meta[String.Format("routine.param[{0}].name", i)] = param.Name;
				meta[String.Format("routine.param[{0}].type", i)] = param.Type;
				meta[String.Format("routine.param[{0}].direction", i)] = param.Direction.ToString();
			}

			return meta;
		}

		private void InjectParameters() {
			foreach (var parameter in RoutineInfo.Parameters) {
				Context.DeclareVariable(new VariableInfo(parameter.Name, parameter.Type, false));
			}
		}

		protected override void ExecuteBlock(BlockExecuteContext context) {
			foreach (var parameter in RoutineInfo.Parameters) {
				var paramName = parameter.Name;
				var paramVar = context.Query.FindVariable(paramName);
				if (!parameter.IsNullable)
					throw new ArgumentException(
						String.Format("The parameter '{0}' of '{1}' is not nullable but it was not found in context", paramName,
							RoutineInfo.RoutineName));

				if (paramVar != null) {
					// TODO: get the value and inject it into the context
				}
			}

			base.ExecuteBlock(context);
		}
	}
}
