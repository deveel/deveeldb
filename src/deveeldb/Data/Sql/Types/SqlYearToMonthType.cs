// 
//  Copyright 2010-2017 Deveel
// 
//    Licensed under the Apache License, Version 2.0 (the "License");
//    you may not use this file except in compliance with the License.
//    You may obtain a copy of the License at
// 
//        http://www.apache.org/licenses/LICENSE-2.0
// 
//    Unless required by applicable law or agreed to in writing, software
//    distributed under the License is distributed on an "AS IS" BASIS,
//    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//    See the License for the specific language governing permissions and
//    limitations under the License.
//


using System;
using System.IO;

namespace Deveel.Data.Sql.Types {
	public sealed class SqlYearToMonthType : SqlType {
		public SqlYearToMonthType()
			: base(SqlTypeCode.YearToMonth) {
		}

		public override bool IsInstanceOf(ISqlValue value) {
			return value is SqlYearToMonth || value is SqlNull;
		}

		public override ISqlValue Add(ISqlValue a, ISqlValue b) {
			if (a is SqlYearToMonth) {
				var x = (SqlYearToMonth)a;

				if (b is SqlYearToMonth) {
					var y = (SqlYearToMonth)b;

					return x.Add(y);
				}
				if (b is SqlNumber) {
					var y = (SqlNumber)b;
					return x.AddMonths((int)y);
				}
			}

			return base.Add(a, b);
		}

		public override ISqlValue Subtract(ISqlValue a, ISqlValue b) {
			if (a is SqlYearToMonth) {
				var x = (SqlYearToMonth)a;

				if (b is SqlYearToMonth) {
					var y = (SqlYearToMonth)b;

					return x.Subtract(y);
				}
				if (b is SqlNumber) {
					var y = (SqlNumber)b;
					return x.AddMonths(-(int)y);
				}
			}

			return base.Add(a, b);
		}
	}
}