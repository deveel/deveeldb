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
using System.Collections;
using System.Text;

namespace Deveel.Data.Procedures {
	/// <summary>
	/// This class represents a special kind of function
	/// which is defined by the user, stored into a database 
	/// and dynamically executed upon user command.
	/// </summary>
	[Serializable]
	public class StoredProcedure {
		public StoredProcedure(ProcedureName name, IList parameters, TType returnType) {
			this.name = name;

			if (parameters != null) {
				int sz = parameters.Count;
				this.parameters = new ArrayList(sz);
				for (int i = 0; i < sz; i++) {
					object parameter = parameters[i];
					if (!(parameter is ProcedureParameter))
						throw new ArgumentException("The element " + i + " of the parameters list is not a parameter.");

					parameters.Add(parameter);
				}
			}

			body = new ProcedureBody(this);
			this.returnType = returnType;
		}

		public StoredProcedure(ProcedureName name, TType returnType)
			: this(name, null, returnType) {
		}

		public StoredProcedure(ProcedureName name, IList parameters)
			: this(name, parameters, null) {
		}

		private readonly TType returnType;
		private readonly ProcedureName name;
		private readonly ArrayList parameters;
		private readonly ProcedureBody body;
		private bool readOnly;
		private readonly StringBuilder text = new StringBuilder();

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

			for (int i = 0; i < parameters.Count; i++) {
				ProcedureParameter parameter = (ProcedureParameter)parameters[i];
				if (parameter.Name == parameterName)
					return parameter;
			}

			return null;
		}

		public ProcedureResult Invoke(Expression[] args, IVariableResolver resolver, IQueryContext context) {
			ProcedureQueryContext queryContext = new ProcedureQueryContext(this, context);

			if (parameters != null && parameters.Count > 0) {
				if (args == null || args.Length != parameters.Count)
					throw new ProcedureException("The procedure '" + name + "' was invoked with an invalid number of arguments.");

				for (int i = 0; i < parameters.Count; i++) {
					Expression arg = args[i];
					ProcedureParameter parameter = (ProcedureParameter)parameters[i];

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
					for (int i = 0; i < parameters.Count; i++) {
						ProcedureParameter parameter = (ProcedureParameter)parameters[i];
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