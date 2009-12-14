using System;

namespace Deveel.Data.Client {
	public sealed class DeveelDbAuthenticationException : DeveelDbException {
		internal DeveelDbAuthenticationException(string accessType, string tableName)
			: base("User doesn't have enough privs to " + accessType + " table " + tableName) {
			this.accessType = accessType;
			this.tableName = tableName;
		}

		private readonly string accessType;
		private readonly string tableName;

		public string TableName {
			get { return tableName; }
		}

		public string AccessType {
			get { return accessType; }
		}
	}
}