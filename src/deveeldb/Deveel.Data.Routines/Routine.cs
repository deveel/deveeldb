// 
//  Copyright 2010-2016 Deveel
// 
//    Licensed under the Apache License, Version 2.0 (the "License");
//    you may not use this file except in compliance with the License.
//    You may obtain a copy of the License at
// 
//        http://www.apache.org/licenses/LICENSE-2.0
// 
//    Unless required by applicable law or agreed to in writing, software
//    distributed under the License is distributed on an "AS IS" BASIS,
//    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//    See the License for the specific language governing permissions and
//    limitations under the License.
//


using System;
using System.Collections.Generic;
using System.Linq;

using Deveel.Data.Diagnostics;
using Deveel.Data.Sql;
using Deveel.Data.Sql.Types;
using Deveel.Data.Sql.Variables;

namespace Deveel.Data.Routines {
	public abstract class Routine : IRoutine {
		protected Routine(RoutineInfo routineInfo) {
			if (routineInfo == null)
				throw new ArgumentNullException("routineInfo");

			RoutineInfo = routineInfo;
		}

		IObjectInfo IDbObject.ObjectInfo {
			get { return RoutineInfo; }
		}

		RoutineType IRoutine.Type {
			get { return RoutineInfo.RoutineType; }
		}

		RoutineInfo IRoutine.RoutineInfo {
			get { return RoutineInfo; }
		}

		protected RoutineInfo RoutineInfo { get; private set; }

		public ObjectName Name {
			get { return RoutineInfo.RoutineName; }
		}

		public RoutineParameter[] Parameters {
			get { return RoutineInfo.Parameters ?? new RoutineParameter[0]; }
		}

		private void PrepareBlock(InvokeArgument[] args, IBlock block) {
			if ((args == null || args.Length == 0) &&
				Parameters.Length == 0)
				return;

			if (args == null || args.Length == 0) {
				if (!Parameters.Any(x => x.IsInput))
					throw new ArgumentException("Invalid number of parameters in the routine invoke.");

				return;
			}

			if (args.Length != Parameters.Count(x => x.IsInput))
				throw new ArgumentException("Invalid number of parameters in the routine invoke.");

			var passedParameters = new List<string>();

			if (args.Any(x => x.IsNamed)) {
				var parameters = Parameters.ToDictionary(x => x.Name, y => y);
				foreach (var argument in args) {
					RoutineParameter parameter;
					if (!parameters.TryGetValue(argument.Name, out parameter))
						throw new ArgumentException(
							String.Format("Invoking routine '{0}' with the named argument '{1}' that is not a parameter of the routine.",
								Name, argument.Name));

					if (!parameter.IsInput)
						throw new ArgumentException(String.Format("Cannot pass any value for parameter '{0}' of routine '{1}'.",
							parameter.Name, Name));

					if (parameter.IsOutput) {
						block.DeclareVariable(parameter.Name, parameter.Type, argument.Value);
					} else {
						block.DeclareConstantVariable(parameter.Name, parameter.Type, argument.Value);
					}

					passedParameters.Add(parameter.Name);
				}
			} else {
				var parameters = Parameters.Where(x => x.IsInput).OrderBy(x => x.Offset).ToArray();
				for (int i = 0; i < parameters.Length; i++) {
					var parameter = parameters[i];
					var argument = args[i];

					if (parameter.IsOutput) {
						block.DeclareVariable(parameter.Name, parameter.Type, argument.Value);
					} else {
						block.DeclareConstantVariable(parameter.Name, parameter.Type, argument.Value);
					}

					passedParameters.Add(parameter.Name);
				}
			}

			var output = Parameters.Where(x => x.IsOutput && !passedParameters.Contains(x.Name));
			foreach (var parameter in output) {
				block.DeclareVariable(parameter.Name, parameter.Type);
			}
		}

		private IDictionary<string, Field> CollectOutput(IBlock block) {
			if (RoutineInfo.Parameters == null || RoutineInfo.Parameters.Length == 0)
				return new Dictionary<string, Field>();

			var outputParams = RoutineInfo.Parameters.Where(x => x.IsOutput).ToArray();
			if (outputParams.Length == 0)
				return new Dictionary<string, Field>();

			var output = new Dictionary<string, Field>(outputParams.Length);

			foreach (var parameter in outputParams) {
				var variable = block.Context.FindVariable(parameter.Name);
				if (variable == null)
					throw new InvalidOperationException(String.Format(
						"Cannot find the output parameter '{0}' in the routine context.", parameter.Name));

				var value = variable.Evaluate(block);
				output[parameter.Name] = value;
			}

			return output;
		}

		public InvokeResult Execute(InvokeContext context) {
			var args = context.Arguments;
			InvokeResult invokeResult = null;

			try {
				context.Request.Context.OnEvent(new RoutineEvent(Name, args, RoutineInfo.RoutineType));

				var block = context.Request.CreateBlock();
				PrepareBlock(context.Arguments, block);

				var result = ExecuteRoutine(block);

				if (RoutineInfo.RoutineType == RoutineType.Function) {
					invokeResult = context.Result(result);
				} else {
					invokeResult = context.Result();
				}

				var output = CollectOutput(block);

				if (output.Count > 0) {
					foreach (var pair in output) {
						context.SetOutput(pair.Key, pair.Value);
					}
				}

				return invokeResult;
			} catch (Exception) {
				throw;
			} finally {
				context.Request.Context.OnEvent(new RoutineEvent(Name, args, RoutineInfo.RoutineType, invokeResult));			}

		}

		protected abstract Field ExecuteRoutine(IBlock context);
	}
}
