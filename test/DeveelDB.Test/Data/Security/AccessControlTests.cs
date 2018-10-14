using System;
using System.Threading.Tasks;

using Deveel.Data.Sql;

using Microsoft.Extensions.DependencyInjection;

using Moq;

using Xunit;

namespace Deveel.Data.Security {
	public class AccessControlTests {
		private const string UserName = "tester";

		private static IContext SetupAccessContext(DbObjectType objType, ObjectName objName, Privilege privilege) {
			var accessController = new Mock<IAccessController>();
			accessController.Setup(x => x.HasPrivilegesAsync(It.Is<string>(u => u == UserName),
				It.Is<DbObjectType>(y => y == objType),
				It.Is<ObjectName>(y => y.Equals(objName)),
				It.Is<Privilege>(y => y.Permits(privilege))))
				.Returns(Task.FromResult(true));

			var services = new ServiceCollection();
			services.AddSingleton<IAccessController>(accessController.Object);

			var mock = new Mock<ISession>();
			mock.SetupGet(x => x.Scope)
				.Returns(services.BuildServiceProvider);
			mock.SetupGet(x => x.User)
				.Returns(new User(UserName));

			return mock.Object;
		}

		[Theory]
		[InlineData(DbObjectType.Table, "sys.tab1", "SELECT, UPDATE", "UPDATE")]
		public static async void AllowedAccess(DbObjectType objType, string objName, string granted, string toCheck) {
			var name = ObjectName.Parse(objName);
			var privObj = SqlPrivileges.Resolver.ResolvePrivilege(granted);
			var checkedPriv = SqlPrivileges.Resolver.ResolvePrivilege(toCheck);

			var context = SetupAccessContext(objType, name, privObj);

			var result = await context.UserHasPrivileges(objType, name, checkedPriv);

			Assert.True(result);
		}

		[Theory]
		[InlineData("sys")]
		public static async void UserCanCreateInSchema(string schemaName) {
			var name = ObjectName.Parse(schemaName);

			var context = SetupAccessContext(DbObjectType.Schema, name, SqlPrivileges.Create);

			var result = await context.UserCanCreateInSchema(schemaName);

			Assert.True(result);
		}
	}
}