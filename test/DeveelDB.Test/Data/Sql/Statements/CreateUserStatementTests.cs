using System;
using System.Threading.Tasks;

using Deveel.Data.Security;
using Deveel.Data.Services;

using Moq;

using Xunit;

namespace Deveel.Data.Sql.Statements {
	public class CreateUserStatementTests {
		private string createdUser;

		private IContext adminContext;
		private IContext userContext;

		public CreateUserStatementTests() {
			var container = new ServiceContainer();

			var securityManager = new Mock<ISecurityManager>();
			securityManager.Setup(x =>
					x.CreateUserAsync(It.IsNotNull<string>(), It.IsNotNull<IUserIdentificationInfo>()))
				.Callback<string, IUserIdentificationInfo>((x, y) => createdUser = x)
				.Returns<string, IUserIdentificationInfo>((x, y) => Task.FromResult(true));

			container.RegisterInstance<ISecurityManager>(securityManager.Object);
			container.Register<IAccessController, PrivilegesCache>();

			var systemContext = new Mock<IContext>();
			systemContext.SetupGet(x => x.Scope)
				.Returns(container);

			var adminSession = new Mock<ISession>();
			adminSession.SetupGet(x => x.User)
				.Returns(User.System);
			adminSession.SetupGet(x => x.ParentContext)
				.Returns(systemContext.Object);
			adminSession.SetupGet(x => x.Scope)
				.Returns(container);

			var userSession = new Mock<ISession>();
			userSession.SetupGet(x => x.User)
				.Returns(new User("user1"));
			userSession.SetupGet(x => x.Scope)
				.Returns(container);
			userSession.SetupGet(x => x.ParentContext)
				.Returns(systemContext.Object);

			adminContext = adminSession.Object;
			userContext = userSession.Object;
		}

		[Theory]
		[InlineData("anto", "1234")]
		public async void AdminCreatesUserWithPassword(string user, string password) {
			var statement = new CreateUserStatement(user, new PasswordIdentificationInfo(password));
			var result = await statement.ExecuteAsync(adminContext);

			Assert.Null(result);
			Assert.Equal(user, createdUser);
		}

		[Theory]
		[InlineData("user2", "1234")]
		public async void UserCreatesOtherUser(string user, string password) {
			var statement = new CreateUserStatement(user, new PasswordIdentificationInfo(password));

			await Assert.ThrowsAsync<UnauthorizedAccessException>(() => statement.ExecuteAsync(userContext));
		}
	}
}