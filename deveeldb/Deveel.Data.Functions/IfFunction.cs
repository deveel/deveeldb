//  
//  IfFunction.cs
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
	// Conditional - IF(a < 0, NULL, a)
	internal sealed class IfFunction : Function {

		public IfFunction(Expression[] parameters)
			: base("if", parameters) {
			if (ParameterCount != 3) {
				throw new Exception(
					"IF function must have exactly three arguments.");
			}
		}

		public override TObject Evaluate(IGroupResolver group, IVariableResolver resolver, IQueryContext context) {
			TObject res = this[0].Evaluate(group, resolver, context);
			if (res.TType is TBooleanType) {
				// Does the result equal true?
				if (res.CompareTo(TObject.GetBoolean(true)) == 0) {
					// Resolved to true so evaluate the first argument
					return this[1].Evaluate(group, resolver, context);
				} else {
					// Otherwise result must evaluate to NULL or false, so evaluate
					// the second parameter
					return this[2].Evaluate(group, resolver, context);
				}
			}
			// Result was not a bool so return null
			return TObject.Null;
		}

		public override TType ReturnTType(IVariableResolver resolver, IQueryContext context) {
			// It's impossible to know the return type of this function until runtime
			// because either comparator could be returned.  We could assume that
			// both branch expressions result in the same type of object but this
			// currently is not enforced.

			// Returns type of first argument
			TType t1 = this[1].ReturnTType(resolver, context);
			// This is a hack for null values.  If the first parameter is null
			// then return the type of the second parameter which hopefully isn't
			// also null.
			if (t1 is TNullType) {
				return this[2].ReturnTType(resolver, context);
			}
			return t1;
		}

	}
}