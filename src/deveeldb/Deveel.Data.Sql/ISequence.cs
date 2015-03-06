// 
//  Copyright 2010-2015 Deveel
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

using Deveel.Data.Sql.Objects;
using Deveel.Data.Transactions;

namespace Deveel.Data.Sql {
	/// <summary>
	/// Represents a numberic sequence in a transaction.
	/// </summary>
	/// <seealso cref="ISimpleTransaction.SetValue"/>
	public interface ISequence : IDbObject {
		/// <summary>
		/// Gets the configuration information of the sequence.
		/// </summary>
		/// <seealso cref="SequenceInfo"/>
		SequenceInfo SequenceInfo { get; }

		SqlNumber GetCurrentValue();

		SqlNumber NextValue();

		SqlNumber SetValue(SqlNumber value);
	}
}
