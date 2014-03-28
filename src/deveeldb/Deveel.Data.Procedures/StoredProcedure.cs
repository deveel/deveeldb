// 
//  Copyright 2010  Deveel
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

using System;
using System.Collections.Generic;
using System.Text;

using Deveel.Data.Types;

namespace Deveel.Data.Procedures {
	/// <summary>
	/// This class represents a special kind of function
	/// which is defined by the user, stored into a database 
	/// and dynamically executed upon user command.
	/// </summary>
	[Serializable]
	public class StoredProcedure {
		private readonly TType returnType;
		private readonly ProcedureName name;
		private readonly List<ProcedureParameter> parameters;
		private readonly ProcedureBody body;
		private bool readOnly;
		private readonly StringBuilder text = new StringBuilder();

		public StoredProcedure(ProcedureName name, IList<ProcedureParameter> parameters, TType returnType) {
			this.name = name;
			this.parameters = new List<ProcedureParameter>(parameters);
			body = new ProcedureBody(this);
			this.returnType = returnType;
		}

		public StoredProcedure(ProcedureName name, TType returnType)
			: this(name, null, returnType) {
		}

		public StoredProcedure(ProcedureName name, IList<ProcedureParameter> parameters)
			: this(name, parameters, null) {
		}

		public ProcedureName ProcedureName {
			get { return name; }
		}

		public TType ReturnType {
			get { return returnType; }
		}

		public ProcedureBody Body {
			get { return body; }
		}

		public bool IsReadOnly {
			get { return readOnly; }
		}

		internal StringBuilder TextBuilder {
			get { return text; }
		}

		public string Text {
			get { return text.ToString(); }
		}

		internal void SetReadOnly() {
			readOnly = true;
		}

		public ProcedureParameter GetParameter(string parameterName) {
			if (parameters == null || parameters.Count == 0)
				return null;

			return parameters.Find(delegate(ProcedureParameter parameter) { return parameter.Name == parameterName; });
		}

		public ProcedureResult Invoke(Expression[] args, IVariableResolver resolver, IQueryContext context) {
			ProcedureQueryContext queryContext = new ProcedureQueryContext(this, context);

			if (parameters != null && parameters.Count > 0) {
				if (args == null || args.Length != parameters.Count)
					throw new ProcedureException("The procedure '" + name + "' was invoked with an invalid number of arguments.");

				for (int i = 0; i < parameters.Count; i++) {
					Expression arg = args[i];
					ProcedureParameter parameter = parameters[i];

					queryContext.DeclareVariable(parameter.Name, parameter.Type, false, !parameter.IsNullable);
					queryContext.SetVariable(parameter.Name, arg);
				}
			}

			ProcedureResult result = new ProcedureResult(this);

			try {
				body.Evaluate(resolver, queryContext);
			} catch (ProcedureException e) {
				result.SetError(e);
			}

			if (!result.IsErrorState) {
				if (parameters != null && parameters.Count > 0) {
					foreach (ProcedureParameter parameter in parameters) {
						if ((parameter.Direction & ParameterDirection.Output) != 0) {
							Variable variable = queryContext.GetVariable(parameter.Name);
							if (variable == null)
								throw new ProcedureException("Unable to retrieve an output variable.");
							result.SetOutputParameter(parameter.Name, variable.Value);
						}
					}
				}
			}

			return result;
		}
	}
}