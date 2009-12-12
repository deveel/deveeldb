//  
//  ProcedureBody.cs
//  
//  Author:
//       Antonello Provenzano <antonello@deveel.com>
// 
//  Copyright (c) 2009 Deveel
// 
//  This program is free software: you can redistribute it and/or modify
//  it under the terms of the GNU General Public License as published by
//  the Free Software Foundation, either version 3 of the License, or
//  (at your option) any later version.
// 
//  This program is distributed in the hope that it will be useful,
//  but WITHOUT ANY WARRANTY; without even the implied warranty of
//  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
//  GNU General Public License for more details.
// 
//  You should have received a copy of the GNU General Public License
//  along with this program.  If not, see <http://www.gnu.org/licenses/>.

using System;
using System.Collections;

using Deveel.Data.Sql;

namespace Deveel.Data.Procedures {
	public sealed class ProcedureBody {
		internal ProcedureBody(StoredProcedure procedure) {
			this.procedure = procedure;
			statements = new ArrayList();
		}

		private readonly StoredProcedure procedure;
		private readonly ArrayList statements;

		public void AddStatement(int index, Statement statement) {
			if (procedure.IsReadOnly)
				throw new InvalidOperationException("The procedure '" + procedure.ProcedureName + "' is immutable.");

			statement.ResolveTree();

			statement.Prepare();
		}

		public void AddStatement(Statement statement) {
			AddStatement(statements.Count, statement);
		}

		internal void Evaluate(IVariableResolver resolver, ProcedureQueryContext context) {
			throw new NotImplementedException();
		}
	}
}