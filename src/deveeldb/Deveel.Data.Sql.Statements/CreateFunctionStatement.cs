using System;
using System.Collections.Generic;

using Deveel.Data.Routines;
using Deveel.Data.Sql.Types;

namespace Deveel.Data.Sql.Statements {
	[Serializable]
	public sealed class CreateFunctionStatement : SqlStatement {
		public CreateFunctionStatement(ObjectName functionName, SqlType returnType, PlSqlBlockStatement body) 
			: this(functionName, returnType, null, body) {
		}

		public CreateFunctionStatement(ObjectName functionName, SqlType returnType, IEnumerable<RoutineParameter> parameters, PlSqlBlockStatement body) {
			if (functionName == null)
				throw new ArgumentNullException("functionName");
			if (returnType == null)
				throw new ArgumentNullException("returnType");
			if (body == null)
				throw new ArgumentNullException("body");

			FunctionName = functionName;
			ReturnType = returnType;
			Parameters = parameters;
			Body = body;
		}

		public ObjectName FunctionName { get; private set; }

		public SqlType ReturnType { get; private set; }

		public IEnumerable<RoutineParameter> Parameters { get; set; }

		public bool ReplaceIfExists { get; set; }

		public PlSqlBlockStatement Body { get; set; }
	}
}
