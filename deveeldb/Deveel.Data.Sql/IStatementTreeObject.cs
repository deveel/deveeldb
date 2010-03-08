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

namespace Deveel.Data.Sql {
	/// <summary>
	/// A complex object that is to be contained within a <see cref="StatementTree"/> object.
	/// </summary>
	/// <remarks>
	/// A statement tree object must be serializable, and it must be able to
	/// reference all <see cref="Expression"/> objects so that they may be prepared.
	/// </remarks>
	internal interface IStatementTreeObject : ICloneable {
		/// <summary>
		/// Prepares all expressions in this statement tree object by 
		/// passing the <see cref="IExpressionPreparer"/> object to the 
		/// <see cref="Expression.Prepare"/> method of the expression.
		/// </summary>
		/// <param name="preparer"></param>
		void PrepareExpressions(IExpressionPreparer preparer);
	}
}