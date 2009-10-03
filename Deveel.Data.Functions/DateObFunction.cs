// 
//  DateObFunction.cs
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
using System.Globalization;

using Deveel.Math;

namespace Deveel.Data.Functions {
	sealed class DateObFunction : Function {

		private readonly static TType DATE_TYPE = new TDateType(SQLTypes.DATE);

		private static readonly string[] formats = new string[] {
		                                                        	"d-MMM-yy",				// the medium format
		                                                        	"M/dd/yy",				// the short format
		                                                        	"MMM dd%, yyy",			// the long format
		                                                        	"dddd, MMM dd%, yyy",	// the full format
		                                                        	"yyyy-MM-dd"			// the SQL format
		                                                        };


		private static TObject DateVal(DateTime d) {
			return new TObject(DATE_TYPE, d);
		}

		public DateObFunction(Expression[] parameters)
			: base("dateob", parameters) {

			if (ParameterCount > 1) {
				throw new Exception("'dateob' function must have only one or zero parameters.");
			}
		}

		public override TObject Evaluate(IGroupResolver group, IVariableResolver resolver, IQueryContext context) {
			// No parameters so return the current date.
			if (ParameterCount == 0) {
				return DateVal(DateTime.Now);
			}

			TObject exp_res = this[0].Evaluate(group, resolver, context);
			// If expression resolves to 'null' then return current date
			if (exp_res.IsNull) {
				return DateVal(DateTime.Now);
			}
				// If expression resolves to a BigDecimal, then treat as number of
				// seconds since midnight Jan 1st, 1970
			else if (exp_res.TType is TNumericType) {
				BigNumber num = (BigNumber)exp_res.Object;
				return DateVal(new DateTime(num.ToInt64()));
			}

			String date_str = exp_res.Object.ToString();

			// We need to synchronize here unfortunately because the Java
			// DateFormat objects are not thread-safe.
			lock (formats) {
				// Try and parse date
				try {
					return DateVal(DateTime.ParseExact(date_str, formats, CultureInfo.CurrentCulture, DateTimeStyles.None));
				} catch {
					throw new Exception("Unable to parse date string '" + date_str + "'");
				}
			}

		}

		public override TType ReturnTType(IVariableResolver resolver, IQueryContext context) {
			return DATE_TYPE;
		}

	}
}