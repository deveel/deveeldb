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
using System.Linq;
using System.Threading.Tasks;

using Deveel.Data.Security;
using Deveel.Data.Services;
using Deveel.Data.Sql.Statements.Security;

using Moq;

using Xunit;

namespace Deveel.Data.Sql.Statements {
	public class SqlStatementTests : IDisposable {
		private IContext context;

		public SqlStatementTests() {
			var container = new ServiceContainer();

			var cache = new PrivilegesCache(null);
			cache.SetObjectPrivileges(ObjectName.Parse("sys.tab1"), "user1", SqlPrivileges.Insert);

			container.RegisterInstance<IAccessController>(cache);

			var mock = new Mock<ISession>();
			mock.Setup(x => x.Scope)
				.Returns(container);
			mock.SetupGet(x => x.User)
				.Returns(new User("user1"));

			context = mock.Object;

		}

		[Fact]
		public async Task ExecuteWithBadprivileges() {
			var requirements = new RequirementCollection();
			requirements.Require(ObjectName.Parse("sys.tab1"), SqlPrivileges.Alter);

			SqlStatement statement = new TestStatement {
				Requirements = requirements,
				Location = new LocationInfo(0, 0)
			};

			statement = await statement.PrepareAsync(context);

			await Assert.ThrowsAnyAsync<UnauthorizedAccessException>(() => statement.ExecuteAsync(context));
		}

		[Fact]
		public async Task ExecutePrivileged() {
			var requirements = new RequirementCollection();
			requirements.Require(ObjectName.Parse("sys.tab1"), SqlPrivileges.Insert);

			SqlStatement statement = new TestStatement {
				Requirements = requirements,
				Location = new LocationInfo(0, 0)
			};

			statement = await statement.PrepareAsync(context);

			await statement.ExecuteAsync(context);
		}

		public void Dispose() {
			context.Dispose();
		}

		#region TestStatement

		class TestStatement : SqlStatement {
			public TestStatement() {
				
			}

			public IEnumerable<IRequirement> Requirements { get; set; }

			public Func<StatementContext, Task> Body { get; set; }

			protected override void Require(IRequirementCollection requirements) {
				requirements.Append(Requirements);
			}

			protected override Task ExecuteStatementAsync(StatementContext context) {
				if (Body == null)
					return Task.CompletedTask;

				return Body(context);
			}
		}

		#endregion
	}
}