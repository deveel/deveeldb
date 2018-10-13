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
using System.Collections.Generic;
using System.Text;

using Microsoft.Extensions.DependencyInjection;

namespace Deveel.Data.Sql.Query {
	public class QueryContext : IContext {
		private IServiceScope scope;

		public QueryContext(IContext parent, IGroupResolver group, IReferenceResolver resolver) {
			ParentContext = parent;
			scope = parent.Scope.CreateScope();
			GroupResolver = group;
			Resolver = resolver;
		}

		public void Dispose() {
			scope?.Dispose();
			scope = null;
		}

		public IContext ParentContext { get; }

		public  IGroupResolver GroupResolver { get; }

		public IReferenceResolver Resolver { get; }

		string IContext.ContextName => "Query";

		IServiceProvider IContext.Scope => scope.ServiceProvider;
	}
}