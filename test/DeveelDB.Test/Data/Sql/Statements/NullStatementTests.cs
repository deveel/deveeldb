using System;
using System.Threading.Tasks;

using Deveel.Data.Services;

using Moq;

using Xunit;

namespace Deveel.Data.Sql.Statements {
	public class NullStatementTests {
		private IContext context;

		public NullStatementTests() {
			var mock = new Mock<IContext>();
			mock.SetupGet(x => x.Scope)
				.Returns(new ServiceContainer());
			context = mock.Object;
		}

		[Fact]
		public async Task ExecuteNull() {
			var statement = new NullStatement();
			var result = await statement.ExecuteAsync(context);

			Assert.Null(result);
		}

		[Fact]
		public void GetString() {
			var statement = new NullStatement();
			Assert.Equal("NULL;", statement.ToString());
		}
	}
}