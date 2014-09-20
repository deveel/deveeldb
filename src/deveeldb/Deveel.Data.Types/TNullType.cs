// 
//  Copyright 2010-2014 Deveel
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
	///<summary>
	/// An implementation of <see cref="TType"/> that represents 
	/// a <c>NULL</c> type.
	///</summary>
	/// <remarks>
	/// A Null type is an object that can't be anything else except null.
	/// </remarks>
	[Serializable]
	public class TNullType : TType {
		public TNullType()
			// There is no SQL type for a query plan node so we make one up here
			: base(SqlType.Null) {
		}

		/// <inheritdoc/>
		public override bool IsComparableType(TType type) {
			return (type is TNullType);
		}

		public override DbType DbType {
			get { return DbType.Object; }
		}

		/// <inheritdoc/>
		public override int Compare(Object ob1, Object ob2) {
			// It's illegal to compare NULL types with this method so we throw an
			// exception here (see method specification).
			throw new ApplicationException("Compare can not compare NULL types.");
		}

		/// <inheritdoc/>
		public override int CalculateApproximateMemoryUse(Object ob) {
			return 16;
		}
	}
}