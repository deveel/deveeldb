//  
//  IdentityFunction.cs
//  
//  Author:
//       Antonello Provenzano <antonello@deveel.com>
//       Tobias Downer <toby@mckoi.com>
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

namespace Deveel.Data.Functions {
	sealed class IdentityFunction : Function {
		public IdentityFunction(Expression[] parameters) 
			: base("identity", parameters) {
		}

		public override TObject Evaluate(IGroupResolver group, IVariableResolver resolver, IQueryContext context) {
			string table_name = this[0].Evaluate(group, resolver, context);
			long v = -1;
			try {
				context.CurrentSequenceValue(table_name);
			} catch(StatementException) {
				if (context is DatabaseQueryContext) {
					v = ((DatabaseQueryContext) context).Connection.CurrentUniqueID(table_name);
				} else {
					throw new InvalidOperationException();
				}
			}

			if (v == -1)
				throw new InvalidOperationException("Unable to determine the sequence of the table " + table_name);

			return TObject.GetInt8(v);
		}

		public override TType ReturnTType(IVariableResolver resolver, IQueryContext context) {
			return TType.NumericType;
		}
	}
}