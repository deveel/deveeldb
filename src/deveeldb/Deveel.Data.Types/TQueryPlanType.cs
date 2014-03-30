// 
//  Copyright 2010  Deveel
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

using System;

using Deveel.Data.Sql;

namespace Deveel.Data.Types {
	/// <summary>
	/// An implementation of TType for a query plan value.
	/// </summary>
	[Serializable]
	public class TQueryPlanType : TType {
		public TQueryPlanType()
			// There is no SQL type for a query plan node so we make one up here
			: base(SqlType.QueryPlanNode) {
		}

		/// <inheritdoc/>
		/// <exception cref="NotSupportedException">
		/// Query plan types cannot be compared.
		/// </exception>
		public override bool IsComparableType(TType type) {
			throw new NotSupportedException("Query Plan types should not be compared.");
		}

		public override DbType DbType {
			get { return DbType.Object; }
		}

		/// <inheritdoc/>
		/// <exception cref="NotSupportedException">
		/// Query plan types cannot be compared.
		/// </exception>
		public override int Compare(Object ob1, Object ob2) {
			throw new NotSupportedException("Query Plan types should not be compared.");
		}

		/// <inheritdoc/>
		public override int CalculateApproximateMemoryUse(Object ob) {
			return 5000;
		}
	}
}