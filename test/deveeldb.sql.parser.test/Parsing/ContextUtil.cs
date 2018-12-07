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
using System.ComponentModel;

using Deveel.Data.Services;
using Deveel.Data.Sql.Expressions;
using Deveel.Data.Sql.Types;

using Moq;

namespace Deveel.Data.Sql.Parsing {
	public static class ContextUtil {
		public static IContext NewParseContext() {
			var container = new ServiceContainer();
			container.RegisterPlSqlParser()
				.RegisterExpressionParser()
				.RegisterTypeParser();

			var systemContext = new Mock<IContext>();
			systemContext.SetupGet(x => x.ContextName)
				.Returns(KnownScopes.System);
			systemContext.SetupGet(x => x.Scope)
				.Returns(container.OpenScope(KnownScopes.System));

			return systemContext.Object;
		}
	}
}