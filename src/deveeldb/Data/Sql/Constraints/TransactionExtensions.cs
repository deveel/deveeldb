// 
//  Copyright 2010-2018 Deveel
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
using System.Linq;

using Deveel.Data.Sql.Tables;
using Deveel.Data.Transactions;

namespace Deveel.Data.Sql.Constraints {
	public static class TransactionExtensions {
		public static void CheckAddConstraintViolations(this ITransaction transaction, ITable table, ConstraintDeferrability deferred) {
			// Get all the rows in the table
			var rows = table.Select(x => x.Number).ToArray();

			// Check the constraints of all the rows in the table.
			CheckAddConstraintViolations(transaction, table, rows, deferred);
		}

		public static void CheckAddConstraintViolations(this ITransaction transaction, ITable table, long[] rowIndices, ConstraintDeferrability deferred) {
			// TODO:
		}

		public static void CheckRemoveConstraintViolations(this ITransaction transaction, ITable table, long[] rowIndices, ConstraintDeferrability deferred) {
			// TODO:
		}
	}
}