using System;

namespace Deveel.Data.DbModel {
	public interface IDbGrantableObject : IDbObject {
		void AddPrivilege(DbPrivilege privilege);

		DbPrivilege AddPrivilege(string privilege, string grantor, string grantee, bool grantable);
	}
}