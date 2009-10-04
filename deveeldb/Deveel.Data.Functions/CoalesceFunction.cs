//  
//  CoalesceFunction.cs
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
	// Coalesce - COALESCE(address2, CONCAT(city, ', ', state, '  ', zip))
	internal sealed class CoalesceFunction : Function {

		public CoalesceFunction(Expression[] parameters)
			: base("coalesce", parameters) {
			if (ParameterCount < 1) {
				throw new Exception("COALESCE function must have at least 1 parameter.");
			}
		}

		public override TObject Evaluate(IGroupResolver group, IVariableResolver resolver, IQueryContext context) {
			int count = ParameterCount;
			for (int i = 0; i < count - 1; ++i) {
				TObject res = this[i].Evaluate(group, resolver, context);
				if (!res.IsNull) {
					return res;
				}
			}
			return this[count - 1].Evaluate(group, resolver, context);
		}

		public override TType ReturnTType(IVariableResolver resolver, IQueryContext context) {
			// It's impossible to know the return type of this function until runtime
			// because either comparator could be returned.  We could assume that
			// both branch expressions result in the same type of object but this
			// currently is not enforced.

			// Go through each argument until we find the first parameter we can
			// deduce the class of.
			int count = ParameterCount;
			for (int i = 0; i < count; ++i) {
				TType t = this[i].ReturnTType(resolver, context);
				if (!(t is TNullType)) {
					return t;
				}
			}
			// Can't work it out so return null type
			return TType.NullType;
		}

	}
}