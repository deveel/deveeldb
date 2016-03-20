using System;
using System.Collections.Generic;

using Deveel.Data.Routines;
using Deveel.Data.Sql.Types;

namespace Deveel.Data.Sql.Statements {
	public sealed class CreateExternalFunctionStatement : SqlStatement {
		public CreateExternalFunctionStatement(ObjectName functionName, SqlType returnType, string externalRef) 
			: this(functionName, returnType, null, externalRef) {
		}

		public CreateExternalFunctionStatement(ObjectName functionName, SqlType returnType,  IEnumerable<RoutineParameter> parameters, string externalRef) {
			if (functionName == null)
				throw new ArgumentNullException("functionName");
			if (returnType == null)
				throw new ArgumentNullException("returnType");
			if (String.IsNullOrEmpty(externalRef))
				throw new ArgumentNullException("externalRef");

			FunctionName = functionName;
			ReturnType = returnType;
			Parameters = parameters;
			ExternalReference = externalRef;
		}

		public ObjectName FunctionName { get; private set; }

		public IEnumerable<RoutineParameter> Parameters { get; set; }

		public SqlType ReturnType { get; private set; }

		public bool ReplaceIfExists { get; set; }

		public string ExternalReference { get; private set; }
	}
}
