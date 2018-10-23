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
using System.Threading.Tasks;

namespace Deveel.Data.Sql.Expressions {
	/// <summary>
	/// The contract implemented by objects that specify SQL expressions
	/// that can be prepared for an execution
	/// </summary>
	/// <typeparam name="TResult">The type of object that is returned
	/// from the preparation process</typeparam>
	public interface ISqlExpressionPreparable<TResult> {
		/// <summary>
		/// Prepares the expressions present in the object using
		/// the provided preparer
		/// </summary>
		/// <param name="preparer">The object that can be used to prepare
		/// the expressions of the object</param>
		/// <returns>
		/// Returns an object that is the result of the preparation of the
		/// SQL expressions contained in the object
		/// </returns>
		TResult Prepare(ISqlExpressionPreparer preparer);
	}
}