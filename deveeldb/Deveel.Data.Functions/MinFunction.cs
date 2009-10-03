// 
//  MinFunction.cs
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
	internal sealed class MinFunction : AggregateFunction {

		public MinFunction(Expression[] parameters)
			: base("min", parameters) {
		}

		protected override TObject EvalAggregate(IGroupResolver group, IQueryContext context,
		                                      TObject ob1, TObject ob2) {
			// This will find min,
			if (ob1 != null) {
				if (ob2.IsNull) {
					return ob1;
				} else {
					if (!ob1.IsNull && ob1.CompareToNoNulls(ob2) < 0) {
						return ob1;
					} else {
						return ob2;
					}
				}
			}
			return ob2;
		}

		public override TType ReturnTType(IVariableResolver resolver, IQueryContext context) {
			// Set to return the same type object as this variable.
			return this[0].ReturnTType(resolver, context);
		}

	}
}