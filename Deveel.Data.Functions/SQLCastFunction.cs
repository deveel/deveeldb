// 
//  SQLCastFunction.cs
//  
//  Author:
//       Antonello Provenzano <antonello@deveel.com>
//       Tobias Downer <toby@mckoi.com>
//  
//  Copyright (c) 2009 Deveel
// 
//  This program is free software: you can redistribute it and/or modify
//  it under the terms of the GNU Lesser General Public License as published by
//  the Free Software Foundation, either version 3 of the License, or
//  (at your option) any later version.
// 
//  This program is distributed in the hope that it will be useful,
//  but WITHOUT ANY WARRANTY; without even the implied warranty of
//  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
//  GNU Lesser General Public License for more details.
// 
//  You should have received a copy of the GNU Lesser General Public License
//  along with this program.  If not, see <http://www.gnu.org/licenses/>.

using System;

namespace Deveel.Data.Functions {
	sealed class SQLCastFunction : Function {

		private readonly TType cast_to_type;

		public SQLCastFunction(Expression[] parameters)
			: base("sql_cast", parameters) {

			// Two parameters - the value to cast and the type to cast to (encoded)
			if (ParameterCount != 2) {
				throw new Exception("'sql_cast' function must have only 2 arguments.");
			}

			// Get the encoded type and parse it into a TType object and cache
			// locally in this object.  We expect that the second parameter of this
			// function is always constant.
			Expression exp = parameters[1];
			if (exp.Count != 1) {
				throw new Exception(
					"'sql_cast' function must have simple second parameter.");
			}

			Object vob = parameters[1].Last;
			if (vob is TObject) {
				TObject ob = (TObject)vob;
				String encoded_type = ob.Object.ToString();
				cast_to_type = TType.DecodeString(encoded_type);
			} else {
				throw new Exception("'sql_cast' function must have simple second parameter.");
			}
		}

		public override TObject Evaluate(IGroupResolver group, IVariableResolver resolver, IQueryContext context) {
			TObject ob = this[0].Evaluate(group, resolver, context);
			// If types are the same then no cast is necessary and we return this
			// object.
			if (ob.TType.SQLType == cast_to_type.SQLType) {
				return ob;
			}
			// Otherwise cast the object and return the new typed object.
			Object casted_ob = TType.CastObjectToTType(ob.Object, cast_to_type);
			return new TObject(cast_to_type, casted_ob);

		}

		public override TType ReturnTType(IVariableResolver resolver, IQueryContext context) {
			return cast_to_type;
		}

	}
}