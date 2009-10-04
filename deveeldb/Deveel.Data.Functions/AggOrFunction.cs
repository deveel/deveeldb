//  
//  AggOrFunction.cs
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
	internal sealed class AggOrFunction : AggregateFunction {

		public AggOrFunction(Expression[] parameters)
			: base("aggor", parameters) {
		}

		protected override TObject EvalAggregate(IGroupResolver group, IQueryContext context, TObject ob1, TObject ob2) {
			// Assuming bitmap numbers, this will find the result of or'ing all the
			// values in the aggregate set.
			if (ob1 != null) {
				if (ob2.IsNull) {
					return ob1;
				} else {
					if (!ob1.IsNull) {
						return ob1.Or(ob2);
					} else {
						return ob2;
					}
				}
			}
			return ob2;
		}

	}
}