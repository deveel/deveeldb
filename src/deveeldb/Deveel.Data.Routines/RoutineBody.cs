// 
//  Copyright 2010-2015 Deveel
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

using Deveel.Data.Sql.Objects;
using Deveel.Data.Sql.Statements;

namespace Deveel.Data.Routines {
	public sealed class RoutineBody {
		private readonly ICollection<SqlStatement> declarations;
		private readonly ICollection<SqlStatement> statements;
 
		internal RoutineBody(RoutineInfo routineInfo) {
			if (routineInfo == null)
				throw new ArgumentNullException("routineInfo");

			RoutineInfo = routineInfo;
			declarations = new List<SqlStatement>();
			statements = new List<SqlStatement>();
		}

		public RoutineInfo RoutineInfo { get; private set; }

		public IEnumerable<SqlStatement> Declarations {
			get { return declarations.AsEnumerable(); }
		}

		public IEnumerable<SqlStatement> Statements {
			get { return statements.AsEnumerable(); }
		}

		public void AddDeclaration(SqlStatement statement) {
			if (statement == null)
				throw new ArgumentNullException("statement");

			AssertIsUserDefined();
			declarations.Add(statement);
		}

		private void AssertIsUserDefined() {
			if (RoutineInfo is FunctionInfo && ((FunctionInfo) RoutineInfo).FunctionType != FunctionType.UserDefined) {
				throw new InvalidOperationException(String.Format(
					"Function '{0}' is not user-defined and cannot declare any body.", RoutineInfo.RoutineName));
			}
			if (RoutineInfo is ProcedureInfo && ((ProcedureInfo)RoutineInfo).ProcedureType != ProcedureType.UserDefined)
				throw new InvalidOperationException(String.Format("The procedure '{0}' is not user-defined and cannot declare any body.", RoutineInfo.RoutineName));

			throw new NotImplementedException();
		}

		public void AddStatement(SqlStatement statement) {
			if (statement == null)
				throw new ArgumentNullException("statement");

			AssertIsUserDefined();
			statements.Add(statement);
		}

		internal ExecuteResult Execute(ExecuteContext context) {
			throw new NotImplementedException();
		}

		internal SqlBinary AsBinary() {
			throw new NotImplementedException();
		}

		internal void FromBinary(SqlBinary binary) {
			throw new NotImplementedException();
		}
	}
}
