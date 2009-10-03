// 
//  AvgFunction.cs
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
	internal sealed class AvgFunction : AggregateFunction {

		public AvgFunction(Expression[] parameters)
			: base("avg", parameters) {
		}

		protected override TObject EvalAggregate(IGroupResolver group, IQueryContext context,
		                                      TObject ob1, TObject ob2) {
			// This will sum,
			if (ob1 != null) {
				if (ob2.IsNull) {
					return ob1;
				} else {
					if (!ob1.IsNull) {
						return ob1.Add(ob2);
					} else {
						return ob2;
					}
				}
			}
			return ob2;
		}

		protected override TObject PostEvalAggregate(IGroupResolver group, IQueryContext context, TObject result) {
			// Find the average from the sum result
			if (result.IsNull) {
				return result;
			}
			return result.Divide(TObject.GetInt4(group.Count));
		}

	}
}