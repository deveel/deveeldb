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

using Deveel.Data.DbSystem;

namespace Deveel.Data.Sql.Expressions {
	/// <summary>
	/// Encapsulates the elements needed to evaluate an <see cref="SqlExpression"/>
	/// </summary>
	public sealed class EvaluateContext {
		public EvaluateContext(IQueryContext queryContext, IVariableResolver variableResolver, IGroupResolver groupResolver) {
			GroupResolver = groupResolver;
			VariableResolver = variableResolver;
			QueryContext = queryContext;
		}

		public IVariableResolver VariableResolver { get; private set; }

		public IGroupResolver GroupResolver { get; private set; }

		public IQueryContext QueryContext { get; private set; }
	}
}