//  
//  CountFunction.cs
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
	internal sealed class CountFunction : Function {
		public CountFunction(Expression[] parameters)
			: base("count", parameters) {
			SetAggregate(true);

			if (ParameterCount != 1) {
				throw new Exception("'count' function must have one argument.");
			}
		}

		public override TObject Evaluate(IGroupResolver group, IVariableResolver resolver,
		                                 IQueryContext context) {
			if (group == null) {
				throw new Exception(
					"'count' can only be used as an aggregate function.");
			}

			int size = group.Count;
			TObject result;
			// if, count(*)
			if (size == 0 || IsGlob) {
				result = TObject.GetInt4(size);
			} else {
				// Otherwise we need to count the number of non-null entries in the
				// columns list(s).

				int total_count = size;

				Expression exp = this[0];
				for (int i = 0; i < size; ++i) {
					TObject val =
						exp.Evaluate(null, group.GetVariableResolver(i), context);
					if (val.IsNull) {
						--total_count;
					}
				}

				result = TObject.GetInt4(total_count);
			}

			return result;
		}

	}
}