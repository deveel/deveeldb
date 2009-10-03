// 
//  TimeStampObFunction.cs
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
	sealed class TimeStampObFunction : Function {

		private readonly static TType TIMESTAMP_TYPE =
			new TDateType(SQLTypes.TIMESTAMP);

		public TimeStampObFunction(Expression[] parameters)
			: base("timestampob", parameters) {

			if (ParameterCount > 1) {
				throw new Exception(
					"'timestampob' function must have only one or zero parameters.");
			}
		}

		public override TObject Evaluate(IGroupResolver group, IVariableResolver resolver,
		                                 IQueryContext context) {

			// No parameters so return the current time.
			if (ParameterCount == 0) {
				return new TObject(TIMESTAMP_TYPE, DateTime.Now);
			}

			TObject exp_res = this[0].Evaluate(group, resolver, context);
			// If expression resolves to 'null' then return current date
			if (exp_res.IsNull) {
				return new TObject(TIMESTAMP_TYPE, DateTime.Now);
			}

			String date_str = exp_res.Object.ToString();

			return new TObject(TIMESTAMP_TYPE, CastHelper.ToTimeStamp(date_str));

		}

		public override TType ReturnTType(IVariableResolver resolver, IQueryContext context) {
			return TIMESTAMP_TYPE;
		}

	}
}